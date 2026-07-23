# Query 结束后内存残留检测计划

## 1. 背景

Databend Query 使用 `TrackingGlobalAllocator` 作为全局分配器，其核心组合为：

```text
GlobalAllocator<MetaTrackerAllocator<DefaultAllocator>>
```

当分配大小大于等于 `META_TRACKER_THRESHOLD`（当前为 512B）时，`MetaTrackerAllocator` 会在用户内存尾部写入分配时所属 `MemStat` 的地址。该地址来自一份 `Arc<MemStat>` 强引用：

```rust
let address = Arc::into_raw(stat.clone()) as usize;
```

释放内存时，allocator 从 metadata 恢复该 `Arc<MemStat>`，并把内存从原 Query 的统计中扣除。这保证了内存即使在其他线程、其他异步任务或者 Query 结束后释放，仍然能够归属到最初的 Query。

现有机制已经具备检测 Query 结束后内存残留的基础，但目前没有维护“已结束 Query”的观察窗口，也没有对残留内存进行延迟采样和告警。

## 2. 目标

实现一个低开销、默认安全的 Query 内存残留检测机制：

1. Query 逻辑结束后继续观察其 `MemStat`。
2. 区分正常的异步延迟释放和长时间稳定的疑似内存泄漏。
3. 输出可用于定位 Query 的日志和 metrics。
4. 不改变正常内存分配、释放和 OOM 行为。
5. 不在第一阶段引入 allocation backtrace 等高开销诊断。

该机制报告的是：

> Query 结束后仍归属于该 Query 的 tracked memory，且经过宽限期后长时间没有明显下降。

告警名称和文案应使用 `retained memory` 或 `suspected memory leak`，不把单次非零采样直接定义为确定的内存泄漏。

## 3. 非目标

第一版不解决以下问题：

1. 不精确追踪小于 512B 的 allocation；它们目前只进入全局统计。
2. 不检测未正确传播 `TrackingPayload` 的后台任务所分配的内存。
3. 不把 jemalloc retained pages、fragmentation 或 RSS 偏高判断为 Query 泄漏。
4. 不输出具体对象类型、源码行或 allocation backtrace。
5. 不因检测结果主动终止 Query、panic 或重启服务。
6. 不要求 Query 完成瞬间 `memory_usage == 0`。

## 4. 现有机制与约束

### 4.1 Query 内存归属

相关代码：

- `src/common/base/src/mem_allocator/tracker.rs`
- `src/common/base/src/runtime/runtime_tracker.rs`
- `src/common/base/src/runtime/memory/mem_stat.rs`
- `src/common/base/src/runtime/memory/stat_buffer_mem_stat.rs`

分配路径：

```text
ThreadTracker::mem_stat()
        -> MemStatBuffer::alloc()
        -> allocation metadata 保存 Arc<MemStat> 地址
        -> inner allocator
```

释放路径：

```text
allocation metadata
        -> Arc::from_raw(MemStat pointer)
        -> MemStatBuffer::dealloc()
        -> inner allocator
```

因此，只要带 metadata 的 Query allocation 尚未释放，对应 `MemStat` 就会继续持有至少一个强引用。

### 4.2 统计存在延迟

`MemStatBuffer` 是线程本地 buffer，当前 flush 阈值为 4MiB。`MemStat::get_memory_usage()` 只读取已经 flush 到 `MemStat.used` 的值，不能直接看到其他线程尚未 flush 的增量。

这意味着：

- Query 结束时的单次读数不可靠；
- 检测必须等待 Query task/executor 正常停止；
- 检测应使用宽限期和多次采样，而不是即时断言；
- 不能尝试从一个线程强制 flush 其他线程的 TLS buffer。

### 4.3 `Weak<MemStat>` 的当前冲突

正式监控应保存 `Weak<MemStat>`，避免监控器自身延长 Query `MemStat` 生命周期。但当前 `MemStatBuffer::dealloc()` 有：

```rust
debug_assert_eq!(Arc::weak_count(mem_stat), 0, ...);
```

引入监控用 `Weak<MemStat>` 前，需要确认该断言的设计目的，并证明移除或放宽它不会破坏 `Arc::into_raw` / `Arc::from_raw` 的生命周期安全。

不建议监控器长期保存 `Arc<MemStat>`，因为：

- 它会让 `MemStat` 无法自然析构；
- 它会干扰 `Arc::strong_count(mem_stat) == 1` 的释放 flush 优化；
- 它无法直接把“监控器引用”和“真实残留引用”区分开。

## 5. 总体设计

新增一个进程内的已结束 Query 内存监控器，暂称：

```rust
FinishedQueryMemoryMonitor
```

### 5.1 生命周期

```text
Query running
    |
    | Query finish，executor/task 已完成正常 shutdown
    v
register(query_id, Weak<MemStat>, metadata)
    |
    | 后台周期采样
    v
Weak::upgrade()
    |-- None: MemStat 已销毁，清理记录
    |
    `-- Some(mem_stat): 读取 current/peak，更新趋势
             |
             | grace period 内：只观测
             | 超过 grace period 且残留较大：记录 warning/metric
             | 长时间稳定不降：报告 suspected leak
             ` 最长观察时间到达：报告最终状态并清理或降频观察
```

### 5.2 监控记录

建议的数据结构：

```rust
struct FinishedQueryMemoryEntry {
    query_id: String,
    query_kind: String,
    finished_at: Instant,
    mem_stat: Weak<MemStat>,
    peak_bytes: usize,
    last_bytes: usize,
    last_sample_at: Instant,
    stable_since: Option<Instant>,
    warning_emitted: bool,
}
```

可选 metadata 应保持小而稳定，避免监控器持有 `QueryContext`、SQL plan、executor 或较大的 Query 字符串。SQL 文本默认不进入该结构；需要关联时使用 `query_id` 查询现有 query log。

### 5.3 并发模型

推荐使用单一后台任务管理 registry：

- Query 完成路径通过有界 channel 发送注册事件；
- 后台任务独占 `HashMap<QueryId, FinishedQueryMemoryEntry>`；
- 避免 allocator 路径访问该 registry；
- registry 设置最大容量；
- channel 满或 registry 超限时只增加 dropped metric，不阻塞 Query 完成路径；
- 服务 shutdown 时停止任务，不等待所有 `MemStat` 归零。

如果现有 service lifecycle 中已有合适的周期任务框架，优先复用，而不是新增独立线程。

## 6. 判定规则

第一版采用可配置或内部常量的保守规则。建议初始值：

```text
sample_interval       = 10s
initial_grace_period  = 30s
warning_threshold     = 16MiB
stable_window         = 60s
minimum_drop_ratio    = 5%
max_observation       = 10min
registry_capacity     = 10_000
```

这些值在实现前需要结合 Query 延迟释放的真实数据确认。第一版可先隐藏在实验性配置或默认关闭的 feature/config 下。

### 6.1 状态分类

#### Released

```text
Weak::upgrade() == None
```

含义：`MemStat` 已经没有强引用，对应 tracking graph 和带 metadata 的 allocation 均已释放。

动作：删除 registry 记录，增加 released counter。

#### Tracking object retained

```text
Weak::upgrade() == Some(mem_stat)
&& current_bytes == 0
```

含义：仍有 `TrackingPayload`、future、context、子 `MemStat` 等引用，但没有已 flush 的大块内存残留。

动作：默认不作为内存泄漏告警；可以单独记录存活时间 metric，超过较长时间后输出 debug/warn。

#### Delayed release

```text
current_bytes > threshold
&& 采样值持续明显下降
```

含义：正常的异步清理或延迟析构。

动作：继续观察，不报警。

#### Suspected memory leak

满足全部条件：

```text
elapsed >= initial_grace_period
current_bytes >= warning_threshold
在 stable_window 内下降比例 < minimum_drop_ratio
```

动作：输出一次结构化 warning，并增加 suspected leak metrics。后续采样避免重复刷日志；只有状态显著变化或达到最长观察时间时再记录。

### 6.2 最终读数不能要求绝对精确

由于 TLS buffering，判断应允许小范围误差：

- 对 bytes 使用阈值，不比较是否严格等于 0；
- 主要依赖 `Weak` 生命周期和大内存量级；
- 采样趋势使用比例与绝对变化量的组合；
- 释放后的负数会被 `get_memory_usage()` 截断到 0，不能据此计算精确净分配数量。

## 7. 实施阶段

### 阶段 0：确认 Query 完成边界

在修改代码前确认各种协议的 Query 完成路径：

- MySQL query
- HTTP query
- Flight/distributed query fragment
- streaming load
- cancel/error/OOM 路径

目标是找到公共、且发生在 executor/task shutdown 之后的注册点。优先放在统一 Query 生命周期层；如果不存在统一入口，应明确列出各入口并避免遗漏异常路径。

验收条件：

- 成功、失败、取消和 OOM 均能注册一次；
- coordinator 和 worker fragment 的语义明确；
- 同一 `query_id` 重复注册有确定处理策略；
- 不提前于 result stream 或 executor 的合法生命周期注册。

### 阶段 1：验证 `Weak<MemStat>` 安全性

1. 为 allocator 添加测试，覆盖存在 `Weak<MemStat>` 时的 allocate/deallocate。
2. 覆盖跨线程释放和 Query tracking guard 已退出的情况。
3. 覆盖 allocation 是 `MemStat` 最后一个 strong reference 的情况。
4. 覆盖 grow、grow_zeroed、shrink 跨 512B 阈值。
5. 确认 `Weak::upgrade()` 在最后一块 allocation 释放后最终返回 `None`。
6. 在测试证明生命周期正确后，移除或调整 `weak_count == 0` debug assertion，并补充注释说明允许监控用途的 `Weak`。

验收条件：

- metadata 中每个 `Arc::into_raw` 仍有且只有一个对应 `Arc::from_raw`；
- 不出现 UAF、double drop 或强引用泄漏；
- debug 和 release 测试行为一致；
- 不改变 allocator 统计回滚逻辑。

### 阶段 2：实现监控核心

新增 monitor 和 registry：

1. 注册已结束 Query 的 `Weak<MemStat>`。
2. 周期执行 `Weak::upgrade()` 和内存采样。
3. 按状态机更新 entry。
4. 按容量、最长观察时间清理。
5. 加入 shutdown 处理。
6. allocator 热路径不增加 registry lookup、锁或日志。

建议先将 monitor 放在 query service 层，而不是 `common/base`：

- `common/base` 提供通用 `MemStat` 和 allocator 能力；
- query service 拥有 `query_id`、Query finish 语义、配置、日志和 service lifecycle；
- 避免 base crate 反向依赖 Query 概念。

具体文件位置应在阶段 0 确认公共生命周期入口后决定。

### 阶段 3：接入 Query 生命周期

1. Query 创建时继续使用现有 `MemStat`。
2. Query 完成并完成正常 shutdown 后，把 `query_id`、`query_kind`、`Arc::downgrade(mem_stat)` 注册到 monitor。
3. 立即释放完成路径持有的额外强引用。
4. 保证每个本地 Query/fragment 至多注册一次。
5. 分布式 Query 按 node 分别检测，本地日志携带 `node_id`；coordinator 不直接把远端统计当作本地 allocation。

### 阶段 4：日志和 metrics

结构化 warning 至少包含：

```text
query_id
node_id
query_kind
elapsed_ms
current_bytes
peak_bytes
previous_bytes
decrease_bytes
decrease_ratio
mem_stat_strong_count
```

建议 metrics：

```text
query_memory_monitor_entries
query_memory_monitor_dropped_total
query_memory_released_after_finish_total
query_memory_retained_queries
query_memory_retained_bytes
query_memory_retained_seconds
query_tracking_object_retained_total
```

注意：

- `Arc::strong_count()` 仅用于诊断，不能解释为 allocation 数量；
- metrics label 不应直接使用 `query_id`，避免高基数；
- `query_id` 只进入日志；
- retained bytes gauge 应清晰定义为当前 registry 中疑似泄漏 entry 的合计值。

### 阶段 5：配置与上线策略

建议配置项：

```text
query_memory_retention_monitor_enabled
query_memory_retention_grace_period_secs
query_memory_retention_warning_bytes
query_memory_retention_stable_secs
query_memory_retention_max_observation_secs
query_memory_retention_registry_capacity
```

上线顺序：

1. 测试环境默认开启，仅记录 debug/info。
2. 收集正常 Query 的残留分布和释放延迟。
3. 调整 grace period 与 warning threshold。
4. 生产环境小范围开启 warning 和 metrics。
5. 确认无明显误报和性能回归后扩大范围。

第一版不让该配置影响 allocator 行为和 Query 执行结果。

## 8. 测试计划

### 8.1 `common/base` allocator 单元测试

覆盖：

1. `Weak<MemStat>` 存在时分配、释放大块内存。
2. Query guard 退出后跨线程释放。
3. 多块 allocation 分批释放，`MemStat` 生命周期正确。
4. 最后一块 allocation 释放后 `Weak::upgrade()` 失败。
5. 小于 512B allocation 不维持 Query `MemStat` 生命周期。
6. 大内存 metadata 为 0 的全局路径不受影响。
7. small -> large、large -> large、large -> small。
8. `grow_zeroed` 正确清理旧 metadata 区域。
9. inner allocator 失败时统计与引用计数回滚。

### 8.2 monitor 单元测试

使用可注入 clock 或手动 tick，避免依赖真实 sleep：

1. grace period 内不报警。
2. `Weak` 失效后及时删除。
3. bytes 持续下降判定为 delayed release。
4. bytes 长时间稳定判定为 suspected leak。
5. 低于阈值不报警。
6. `used == 0` 但 `Weak` 存活时分类正确。
7. warning 只输出一次，不重复刷日志。
8. registry 超限时不阻塞并记录 dropped counter。
9. max observation 到期后的清理策略正确。
10. shutdown 不挂起。

### 8.3 Query service 集成测试

1. 正常 Query 结束后 allocation 全部释放，不产生告警。
2. 人工保留一个大 buffer，Query 结束后产生 retained warning。
3. buffer 在 grace period 内释放，不产生告警。
4. buffer 在首次 warning 后释放，registry 和 gauge 恢复。
5. Query cancel 路径被监控。
6. Query error/OOM 路径被监控。
7. 分布式 Query 的 coordinator/worker 记录不混淆。
8. HTTP result 生命周期不会造成稳定误报。

测试钩子应避免在生产代码中永久泄漏内存；测试结束时必须释放人工保留对象。

### 8.4 性能验证

重点确认：

- allocation/deallocation 热路径没有新增锁和 registry 操作；
- Query finish 路径只执行一次 `Arc::downgrade` 和非阻塞事件发送；
- 后台扫描 CPU 与 registry 大小成线性关系且有容量上限；
- 正常无残留 Query 可以快速从 registry 删除；
- 监控关闭时开销接近零。

## 9. 验证命令

根据最终涉及 crate 选择最小范围命令，建议顺序：

```bash
# common/base allocator 与 memory tests
cargo test -p databend-common-base mem_allocator::tracker
cargo test -p databend-common-base runtime::memory

# query service 新增 monitor 单元/集成测试
cargo test -p databend-query-service <monitor_test_filter>

# 格式检查
cargo fmt --all -- --check

# 相关 crate clippy；具体 package 名以 Cargo.toml 为准
cargo clippy -p databend-common-base -p databend-query-service --all-targets -- -D warnings
```

如果 package 名或 feature 组合与上述命令不符，实现时按 workspace 当前定义调整并在交付说明中记录实际命令。

## 10. 风险与缓解

### 风险 1：误报正常延迟释放

缓解：使用 grace period、多次采样、趋势判断和最小 bytes 阈值；初期仅观测不采取动作。

### 风险 2：TLS buffer 导致读数滞后

缓解：不使用完成瞬间的单点判断；确保 executor/task shutdown；使用较长稳定窗口。

### 风险 3：监控器自身造成生命周期变化

缓解：正式实现仅保存 `Weak<MemStat>`；不长期保存 Query `Arc<MemStat>`、`QueryContext` 或 executor。

### 风险 4：修改 weak-count 假设破坏 unsafe allocator

缓解：先补生命周期和跨线程测试，再修改断言；重点审查 `Arc::into_raw` / `Arc::from_raw` 的一一对应关系。

### 风险 5：registry 无界增长

缓解：有界 channel、容量上限、最长观察时间和 dropped metric。

### 风险 6：大量小对象泄漏漏检

缓解：文档和告警明确这是 tracked large-allocation retention；后续若有需求，再评估采样追踪小 allocation，而不是降低全局 metadata 阈值。

### 风险 7：错误的 tracking context 导致漏归属

缓解：将其作为单独的 context propagation 问题；可通过全局内存增长与 Query retained memory 不匹配发现，但本计划不直接解决。

## 11. 后续增强

完成低开销检测后，可以为已确认的疑似泄漏增加按需诊断模式：

1. 只采样大于指定大小的 allocation。
2. 保存 allocation ID、size 和 backtrace hash。
3. 按 Query 与调用栈聚合未释放样本。
4. 与 jemalloc heap profiling 结果关联。
5. 允许运维针对单个 Query 或时间窗口开启，避免全局持续开销。

该增强不应阻塞第一版 retained-memory monitor。

## 12. 完成标准

本计划完成时应满足：

- [ ] Query 成功、失败、取消和 OOM 结束路径都能注册监控。
- [ ] Monitor 只持有 `Weak<MemStat>`，不会人为延长 Query 生命周期。
- [ ] 存在 `Weak` 时 allocator 的 allocate/deallocate/grow/shrink 已有回归测试。
- [ ] 正常延迟释放不会被单点非零值直接判为泄漏。
- [ ] 长时间稳定的大额残留会输出一次结构化 warning。
- [ ] Registry 有容量和观察时间上限。
- [ ] Metrics 不使用 `query_id` 高基数 label。
- [ ] 监控关闭时不影响 Query 执行和 allocator 热路径。
- [ ] 相关单元测试、集成测试、格式检查和 clippy 通过。
- [ ] 文档明确检测范围：主要覆盖大于等于 512B 且正确绑定 Query tracking context 的 allocation。
