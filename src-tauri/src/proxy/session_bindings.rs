use dashmap::DashMap;
use std::time::{Duration, Instant};

/// 默认最大粘性会话绑定数
pub const DEFAULT_MAX_SESSION_BINDINGS: usize = 50_000;
/// 默认粘性会话空闲过期时间（2 小时）
pub const DEFAULT_SESSION_IDLE_TTL: Duration = Duration::from_secs(7_200);

#[derive(Debug, Clone)]
struct SessionBindingEntry {
    account_id: String,
    last_used: Instant,
}

/// 带容量硬顶与空闲 TTL 驱逐的粘性会话绑定表
#[derive(Debug)]
pub struct SessionBindings {
    entries: DashMap<String, SessionBindingEntry>,
    max_entries: usize,
    idle_ttl: Duration,
}

impl Default for SessionBindings {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionBindings {
    pub fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_SESSION_BINDINGS, DEFAULT_SESSION_IDLE_TTL)
    }

    pub fn with_limits(max_entries: usize, idle_ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            max_entries: max_entries.max(1),
            idle_ttl,
        }
    }

    /// 获取未过期的绑定账号，并刷新 `last_used`；若已超期则清理并返回 `None`
    pub fn get(&self, session_id: &str) -> Option<String> {
        let now = Instant::now();
        let expired = {
            let mut entry = self.entries.get_mut(session_id)?;
            if now.duration_since(entry.last_used) >= self.idle_ttl {
                true
            } else {
                entry.last_used = now;
                return Some(entry.account_id.clone());
            }
        };
        if expired {
            self.entries.remove_if(session_id, |_, e| {
                now.duration_since(e.last_used) >= self.idle_ttl
            });
        }
        None
    }

    /// 原子绑定或返回既有未过期的账号；容量满时先驱逐过期条目，再淘汰最旧批次
    pub fn bind_or_get(&self, session_id: &str, account_id: &str) -> String {
        if !self.entries.contains_key(session_id) && self.entries.len() >= self.max_entries {
            self.evict_if_full();
        }

        let now = Instant::now();
        let mut entry = self
            .entries
            .entry(session_id.to_string())
            .or_insert_with(|| SessionBindingEntry {
                account_id: account_id.to_string(),
                last_used: now,
            });
        if now.duration_since(entry.last_used) >= self.idle_ttl {
            entry.account_id = account_id.to_string();
        }
        entry.last_used = now;
        entry.account_id.clone()
    }

    /// 直接写入/覆盖会话绑定（兼容旧测试与显式重绑场景）
    pub fn insert(&self, session_id: String, account_id: String) {
        if !self.entries.contains_key(&session_id) && self.entries.len() >= self.max_entries {
            self.evict_if_full();
        }
        self.entries.insert(
            session_id,
            SessionBindingEntry {
                account_id,
                last_used: Instant::now(),
            },
        );
    }

    pub fn remove(&self, session_id: &str) -> Option<String> {
        self.entries.remove(session_id).map(|(_, e)| e.account_id)
    }

    pub fn remove_if_matches(&self, session_id: &str, account_id: &str) -> bool {
        self.entries
            .remove_if(session_id, |_, e| e.account_id == account_id)
            .is_some()
    }

    pub fn retain_not_account(&self, account_id: &str) {
        self.entries.retain(|_, e| e.account_id != account_id);
    }

    pub fn contains_key(&self, session_id: &str) -> bool {
        self.entries
            .get(session_id)
            .is_some_and(|e| e.last_used.elapsed() < self.idle_ttl)
    }

    pub fn clear(&self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 清理所有已超过 `idle_ttl` 的条目，返回清理数量
    pub fn evict_expired(&self) -> usize {
        let now = Instant::now();
        let mut removed = 0usize;
        self.entries.retain(|_, e| {
            if now.duration_since(e.last_used) >= self.idle_ttl {
                removed += 1;
                false
            } else {
                true
            }
        });
        removed
    }

    fn evict_if_full(&self) {
        self.evict_expired();
        let current_len = self.entries.len();
        if current_len < self.max_entries {
            return;
        }
        let batch_min = (self.max_entries / 10).max(1);
        let overflow = current_len
            .saturating_sub(self.max_entries)
            .saturating_add(1);
        let evict_count = overflow.max(batch_min).min(current_len);

        let mut snapshots: Vec<(String, Instant)> = self
            .entries
            .iter()
            .map(|r| (r.key().clone(), r.value().last_used))
            .collect();
        snapshots.sort_by_key(|(_, ts)| *ts);
        for (key, _) in snapshots.into_iter().take(evict_count) {
            self.entries.remove(&key);
        }
    }
}
