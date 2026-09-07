// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub(super) struct FunctionSpillPolicy {
    force: bool,
    min_state_bytes: usize,
}

impl FunctionSpillPolicy {
    pub fn new(force: bool, min_state_bytes: usize) -> Self {
        Self {
            force,
            min_state_bytes,
        }
    }

    pub fn should_spill(&self, state_bytes: usize, pressure: bool) -> bool {
        self.force || (pressure && state_bytes != 0 && state_bytes >= self.min_state_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::FunctionSpillPolicy;

    #[test]
    fn ample_memory_keeps_even_large_states_in_memory() {
        let policy = FunctionSpillPolicy::new(false, 64);
        for size in [1, 64, 1024, usize::MAX] {
            assert!(!policy.should_spill(size, false));
        }
    }

    #[test]
    fn repeated_pressure_never_bypasses_state_size() {
        let policy = FunctionSpillPolicy::new(false, 64);
        for _ in 0..4 {
            for size in [0, 1, 32, 63] {
                assert!(!policy.should_spill(size, true));
            }
            assert!(policy.should_spill(64, true));
            assert!(policy.should_spill(1024, true));
            assert!(!policy.should_spill(1024, false));
        }
    }

    #[test]
    fn explicit_force_bypasses_pressure_and_size() {
        let policy = FunctionSpillPolicy::new(true, usize::MAX);
        assert!(policy.should_spill(1, false));
        assert!(policy.should_spill(1, true));
    }
}
