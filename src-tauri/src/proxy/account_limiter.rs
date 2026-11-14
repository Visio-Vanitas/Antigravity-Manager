use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Debug)]
struct AccountSlot {
    in_flight: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl AccountSlot {
    fn new() -> Self {
        Self {
            in_flight: Arc::new(AtomicUsize::new(0)),
            notify: Arc::new(Notify::new()),
        }
    }
}

/// 活跃在途请求 RAII Guard：在请求处理或流式传输期间持有，Drop 时原子释放并发计数并唤醒排队请求
#[derive(Debug)]
pub struct AccountInFlightGuard {
    pub account_id: String,
    counter: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Drop for AccountInFlightGuard {
    fn drop(&mut self) {
        let prev = self.counter.fetch_sub(1, Ordering::SeqCst);
        self.notify.notify_one();
        tracing::debug!(
            "📉 [InFlight] Released request for account {}: {} -> {}",
            self.account_id,
            prev,
            prev.saturating_sub(1)
        );
    }
}

/// 按账号维度的实时并发计数与有界排队限流器
#[derive(Debug, Default)]
pub struct AccountConcurrencyLimiter {
    slots: DashMap<String, Arc<AccountSlot>>,
}

impl AccountConcurrencyLimiter {
    pub fn new() -> Self {
        Self {
            slots: DashMap::new(),
        }
    }

    fn slot_for(&self, account_id: &str) -> Arc<AccountSlot> {
        self.slots
            .entry(account_id.to_string())
            .or_insert_with(|| Arc::new(AccountSlot::new()))
            .clone()
    }

    /// 无锁读取指定账号当前的活跃在途请求数
    pub fn get_in_flight(&self, account_id: &str) -> usize {
        self.slots
            .get(account_id)
            .map(|s| s.in_flight.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// 无硬上限快速路径：原子递增在途计数并返回 RAII Guard
    pub fn acquire_unbounded(&self, account_id: &str) -> AccountInFlightGuard {
        let slot = self.slot_for(account_id);
        let prev = slot.in_flight.fetch_add(1, Ordering::SeqCst);
        tracing::debug!(
            "📈 [InFlight] Acquired request for account {}: {} -> {}",
            account_id,
            prev,
            prev + 1
        );
        AccountInFlightGuard {
            account_id: account_id.to_string(),
            counter: Arc::clone(&slot.in_flight),
            notify: Arc::clone(&slot.notify),
        }
    }

    /// 有界并发获取：
    /// - `max_concurrent == 0` 时直接走无上限快速路径；
    /// - `max_concurrent > 0` 时通过 CAS + `Notify` 在 `queue_timeout` 内异步排队，超时返回 `Err`。
    pub async fn acquire_bounded(
        &self,
        account_id: &str,
        max_concurrent: usize,
        queue_timeout: Duration,
    ) -> Result<AccountInFlightGuard, String> {
        if max_concurrent == 0 {
            return Ok(self.acquire_unbounded(account_id));
        }

        let slot = self.slot_for(account_id);
        let deadline = tokio::time::Instant::now() + queue_timeout;

        loop {
            let notified = slot.notify.notified();
            if Self::try_acquire_slot(&slot.in_flight, max_concurrent) {
                return Ok(AccountInFlightGuard {
                    account_id: account_id.to_string(),
                    counter: Arc::clone(&slot.in_flight),
                    notify: Arc::clone(&slot.notify),
                });
            }

            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "Account {} concurrency limit ({}) reached after waiting {}ms",
                    account_id,
                    max_concurrent,
                    queue_timeout.as_millis()
                ));
            }

            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                if Self::try_acquire_slot(&slot.in_flight, max_concurrent) {
                    return Ok(AccountInFlightGuard {
                        account_id: account_id.to_string(),
                        counter: Arc::clone(&slot.in_flight),
                        notify: Arc::clone(&slot.notify),
                    });
                }
                return Err(format!(
                    "Account {} concurrency limit ({}) reached after waiting {}ms",
                    account_id,
                    max_concurrent,
                    queue_timeout.as_millis()
                ));
            }
        }
    }

    pub fn remove_account(&self, account_id: &str) {
        self.slots.remove(account_id);
    }

    fn try_acquire_slot(counter: &AtomicUsize, max_concurrent: usize) -> bool {
        let mut current = counter.load(Ordering::Acquire);
        loop {
            if current >= max_concurrent {
                return false;
            }
            match counter.compare_exchange_weak(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}
