//! 出口吞吐隔离与热路径静态正则单元测试
//!
//! 覆盖：
//! 1. 高速出口与慢速出口并发写入吞吐样本时互不污染，各自算出的上传预算精确隔离；
//! 2. `reset_upload_throughput_estimate()` 清空所有分桶并保持默认桶兼容；
//! 3. `sanitize_error_for_log` 在并发调用下正确脱敏 `access_token` 与 `Bearer` 凭证。

use crate::proxy::config::UploadTimeoutConfig;
use crate::proxy::upstream::client::{
    current_upload_throughput_bps, current_upload_throughput_bps_for, observe_upload_throughput,
    observe_upload_throughput_for, reset_upload_throughput_estimate, sanitize_error_for_log,
    PayloadMediaKind, UploadBudgetSource, UpstreamClient,
};
use crate::proxy::upstream::throughput_estimator::egress_bucket_count;
use std::sync::{Arc, Mutex};
use std::thread;

static EGRESS_TEST_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn test_egress_throughput_isolation_and_budget_planning() {
    let _guard = EGRESS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_upload_throughput_estimate();

    let fast_egress = "pool:direct-fast";
    let slow_egress = "pool:ultra-reality";
    let fast_bps = 30 * 1024 * 1024; // 30 MB/s
    let slow_bps = 800 * 1024; // 800 KB/s

    // 并发写入高速与慢速出口样本，验证各自 EWMA 互不污染
    let h1 = thread::spawn(move || {
        for _ in 0..32 {
            observe_upload_throughput_for(fast_egress, fast_bps);
        }
    });
    let h2 = thread::spawn(move || {
        for _ in 0..32 {
            observe_upload_throughput_for(slow_egress, slow_bps);
        }
    });
    h1.join().expect("fast egress thread panicked");
    h2.join().expect("slow egress thread panicked");

    let observed_fast = current_upload_throughput_bps_for(fast_egress);
    let observed_slow = current_upload_throughput_bps_for(slow_egress);
    assert_eq!(observed_fast, Some(fast_bps));
    assert_eq!(observed_slow, Some(slow_bps));

    // 配置 10 Mbps (1.25 MB/s) 声明下限，上传 10 MB 请求体：
    // - fast_egress (30 MB/s >= 1.25 MB/s) 按声明下限估算 -> AdaptiveSize
    // - slow_egress (800 KB/s < 1.25 MB/s) 按实测慢速放宽 -> AdaptiveObserved，预算显著更宽松
    let mut cfg = UploadTimeoutConfig::default();
    cfg.min_upload_mbps = 10.0;
    let body_len = 10 * 1024 * 1024;

    let fast_plan = UpstreamClient::plan_upstream_timeouts_with_observed(
        PayloadMediaKind::Image,
        body_len,
        &cfg,
        observed_fast,
    );
    let slow_plan = UpstreamClient::plan_upstream_timeouts_with_observed(
        PayloadMediaKind::Image,
        body_len,
        &cfg,
        observed_slow,
    );

    assert_eq!(fast_plan.budget_source, UploadBudgetSource::AdaptiveSize);
    assert_eq!(
        slow_plan.budget_source,
        UploadBudgetSource::AdaptiveObserved
    );
    assert!(
        slow_plan.upload_budget_secs > fast_plan.upload_budget_secs,
        "slow egress budget ({}) must be wider than fast egress ({})",
        slow_plan.upload_budget_secs,
        fast_plan.upload_budget_secs
    );

    reset_upload_throughput_estimate();
}

#[test]
fn test_reset_clears_all_egress_buckets_and_preserves_default_compat() {
    let _guard = EGRESS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    reset_upload_throughput_estimate();
    assert!(current_upload_throughput_bps().is_none());
    assert_eq!(egress_bucket_count(), 0);

    observe_upload_throughput(2 * 1024 * 1024);
    observe_upload_throughput_for("pool:node-a", 512 * 1024);
    observe_upload_throughput_for("pool:node-b", 12 * 1024 * 1024);

    assert_eq!(current_upload_throughput_bps(), Some(2 * 1024 * 1024));
    assert_eq!(
        current_upload_throughput_bps_for("default"),
        Some(2 * 1024 * 1024)
    );
    assert_eq!(
        current_upload_throughput_bps_for("pool:node-a"),
        Some(512 * 1024)
    );
    assert_eq!(egress_bucket_count(), 3);

    reset_upload_throughput_estimate();
    assert!(current_upload_throughput_bps().is_none());
    assert!(current_upload_throughput_bps_for("pool:node-a").is_none());
    assert!(current_upload_throughput_bps_for("pool:node-b").is_none());
    assert_eq!(egress_bucket_count(), 0);
}

#[test]
fn test_sanitize_error_for_log_concurrent_redaction() {
    let raw = Arc::new(
        "upstream failed: access_token=ya29.secret_abc123 authorization: Bearer tok_xyz_987654 proxy_url=http://user:pass@127.0.0.1:7890"
            .to_string(),
    );

    let handles: Vec<_> = (0..8)
        .map(|_| {
            let input = Arc::clone(&raw);
            thread::spawn(move || {
                for _ in 0..50 {
                    let sanitized = sanitize_error_for_log(&input);
                    assert!(!sanitized.contains("ya29.secret_abc123"));
                    assert!(!sanitized.contains("tok_xyz_987654"));
                    assert!(!sanitized.contains("user:pass@127.0.0.1:7890"));
                    assert!(sanitized.contains("access_token=<redacted>"));
                    assert!(sanitized.contains("Bearer <redacted>"));
                    assert!(sanitized.contains("proxy_url=<redacted>"));
                }
            })
        })
        .collect();

    for h in handles {
        h.join().expect("sanitize_error_for_log thread panicked");
    }
}
