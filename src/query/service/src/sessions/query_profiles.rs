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

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use databend_common_pipeline::core::PlanProfile;
use parking_lot::RwLock;

#[derive(Default)]
pub struct QueryProfiles {
    // PlanProfile ids are local to the executor that produced them. A shared
    // QueryContext can merge batches from nested/internal or remote executors
    // whose node ids all start at 0, and current_executor may already point to
    // a different executor when a batch arrives. profile_execution_id is the
    // stable batch owner used to assign each executor a disjoint global id range.
    profiles: RwLock<HashMap<Option<u32>, PlanProfile>>,
    execution_ranges: RwLock<HashMap<String, ProfileExecutionRange>>,
    next_id: AtomicU32,
}

#[derive(Clone, Copy)]
struct ProfileExecutionRange {
    offset: u32,
    id_count: u32,
}

impl QueryProfiles {
    pub fn get(&self) -> Vec<PlanProfile> {
        self.profiles.read().values().cloned().collect()
    }

    pub fn get_for_execution(&self, profile_execution_id: &str) -> HashMap<u32, PlanProfile> {
        let Some(range) = self
            .execution_ranges
            .read()
            .get(profile_execution_id)
            .copied()
        else {
            return HashMap::new();
        };

        let mut profiles = HashMap::new();
        for profile in self.profiles.read().values() {
            let Some(id) = profile.id else {
                continue;
            };
            if id < range.offset || id >= range.offset + range.id_count {
                continue;
            }

            let mut profile = profile.clone();
            profile.id = Some(id - range.offset);
            profile.parent_id = profile.parent_id.map(|id| id - range.offset);
            profiles.insert(profile.id.unwrap(), profile);
        }
        profiles
    }

    pub fn add(&self, profiles: &HashMap<u32, PlanProfile>) {
        self.merge(profiles.values().cloned());
    }

    pub fn add_with_execution(
        &self,
        profile_execution_id: &str,
        profiles: &HashMap<u32, PlanProfile>,
    ) {
        let Some(range) = self.execution_range(profile_execution_id, profiles) else {
            return;
        };
        self.merge(profiles.values().map(|profile| {
            let mut profile = profile.clone();
            if range.offset != 0 {
                profile.id = profile.id.map(|id| id + range.offset);
                profile.parent_id = profile.parent_id.map(|id| id + range.offset);
            }
            profile
        }));
    }

    fn execution_range(
        &self,
        profile_execution_id: &str,
        profiles: &HashMap<u32, PlanProfile>,
    ) -> Option<ProfileExecutionRange> {
        let mut execution_ranges = self.execution_ranges.write();
        if let Some(range) = execution_ranges.get(profile_execution_id) {
            return Some(*range);
        }

        let id_count = profiles
            .values()
            .filter_map(|profile| profile.id)
            .max()
            .map(|id| id + 1)?;
        let offset = self.next_id.fetch_add(id_count, Ordering::Relaxed);
        let range = ProfileExecutionRange { offset, id_count };
        execution_ranges.insert(profile_execution_id.to_string(), range);
        Some(range)
    }

    fn merge(&self, profiles: impl Iterator<Item = PlanProfile>) {
        let mut merged_profiles = self.profiles.write();

        for query_profile in profiles {
            match merged_profiles.entry(query_profile.id) {
                Entry::Vacant(v) => {
                    v.insert(query_profile);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().merge(&query_profile);
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use databend_common_base::runtime::profile::ProfileLabel;

    use super::*;

    fn plan_profile(id: u32, parent_id: Option<u32>, statistic: usize) -> PlanProfile {
        let mut statistics = std::array::from_fn(|_| 0);
        statistics[0] = statistic;

        PlanProfile {
            id: Some(id),
            name: Some(format!("plan-{id}")),
            parent_id,
            title: Arc::new(format!("plan-{id}")),
            labels: Arc::new(Vec::<ProfileLabel>::new()),
            metrics: BTreeMap::new(),
            statistics,
            errors: vec![],
        }
    }

    fn profiles_by_id(profiles: Vec<PlanProfile>) -> HashMap<u32, PlanProfile> {
        profiles
            .into_iter()
            .map(|profile| (profile.id.unwrap(), profile))
            .collect()
    }

    #[test]
    fn query_profiles_reuses_execution_offset_for_repeated_batches() {
        let query_profiles = QueryProfiles::default();
        let first_batch =
            profiles_by_id(vec![plan_profile(0, None, 1), plan_profile(1, Some(0), 2)]);
        let second_batch =
            profiles_by_id(vec![plan_profile(0, None, 3), plan_profile(1, Some(0), 4)]);

        query_profiles.add_with_execution("exec-a", &first_batch);
        query_profiles.add_with_execution("exec-a", &second_batch);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[&0].statistics[0], 4);
        assert_eq!(profiles[&1].statistics[0], 6);
        assert_eq!(profiles[&1].parent_id, Some(0));

        let raw_profiles = query_profiles.get_for_execution("exec-a");
        assert_eq!(raw_profiles.len(), 2);
        assert_eq!(raw_profiles[&0].statistics[0], 4);
        assert_eq!(raw_profiles[&1].statistics[0], 6);
        assert_eq!(raw_profiles[&1].parent_id, Some(0));
    }

    #[test]
    fn query_profiles_remaps_distinct_executions() {
        let query_profiles = QueryProfiles::default();
        let first_batch =
            profiles_by_id(vec![plan_profile(0, None, 1), plan_profile(1, Some(0), 2)]);
        let second_batch =
            profiles_by_id(vec![plan_profile(0, None, 3), plan_profile(1, Some(0), 4)]);

        query_profiles.add_with_execution("exec-a", &first_batch);
        query_profiles.add_with_execution("exec-b", &second_batch);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 4);
        assert_eq!(profiles[&0].statistics[0], 1);
        assert_eq!(profiles[&1].statistics[0], 2);
        assert_eq!(profiles[&2].statistics[0], 3);
        assert_eq!(profiles[&3].statistics[0], 4);
        assert_eq!(profiles[&1].parent_id, Some(0));
        assert_eq!(profiles[&3].parent_id, Some(2));

        let raw_profiles = query_profiles.get_for_execution("exec-b");
        assert_eq!(raw_profiles.len(), 2);
        assert_eq!(raw_profiles[&0].statistics[0], 3);
        assert_eq!(raw_profiles[&1].statistics[0], 4);
        assert_eq!(raw_profiles[&1].parent_id, Some(0));
    }

    #[test]
    fn query_profiles_ignores_empty_execution_batches() {
        let query_profiles = QueryProfiles::default();
        let empty_batch = HashMap::new();
        let first_batch = profiles_by_id(vec![plan_profile(0, None, 1)]);
        let second_batch = profiles_by_id(vec![plan_profile(0, None, 2)]);

        query_profiles.add_with_execution("exec-a", &empty_batch);
        query_profiles.add_with_execution("exec-b", &first_batch);
        query_profiles.add_with_execution("exec-a", &second_batch);

        let profiles = profiles_by_id(query_profiles.get());
        assert_eq!(profiles.len(), 2);
        assert_eq!(profiles[&0].statistics[0], 1);
        assert_eq!(profiles[&1].statistics[0], 2);
    }
}
