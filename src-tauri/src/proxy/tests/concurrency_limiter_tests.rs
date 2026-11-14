use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::proxy::account_limiter::AccountConcurrencyLimiter;
use crate::proxy::session_bindings::SessionBindings;
use crate::proxy::token_manager::{ProxyToken, TokenManager};

fn make_test_token(account_id: &str, email: &str, timestamp: i64, expires_in: i64) -> ProxyToken {
    ProxyToken {
        account_id: account_id.to_string(),
        priority: 10,
        model_priorities: HashMap::new(),
        access_token: "stale_access_token".to_string(),
        refresh_token: String::new(),
        expires_in,
        timestamp,
        email: email.to_string(),
        account_path: PathBuf::from(format!("/tmp/{}.json", account_id)),
        project_id: Some("test-project".to_string()),
        subscription_tier: Some("PRO".to_string()),
        remaining_quota: Some(100),
        protected_models: HashSet::new(),
        forbidden_models: HashSet::new(),
        health_score: 1.0,
        reset_time: None,
        weekly_reset_time: None,
        validation_blocked: false,
        validation_blocked_until: 0,
        validation_url: None,
        model_quotas: HashMap::new(),
        model_limits: HashMap::new(),
        available_models: HashSet::new(),
        model_metadata: HashMap::new(),
        model_reset_times: HashMap::new(),
        model_weekly_reset_times: HashMap::new(),
        model_quota_windows: HashMap::new(),
    }
}

#[tokio::test]
async fn test_account_limiter_queues_and_wakes_on_drop() {
    let limiter = Arc::new(AccountConcurrencyLimiter::new());
    let g1 = limiter
        .acquire_bounded("acc-1", 2, Duration::from_secs(2))
        .await
        .expect("first slot must succeed");
    let g2 = limiter
        .acquire_bounded("acc-1", 2, Duration::from_secs(2))
        .await
        .expect("second slot must succeed");
    assert_eq!(limiter.get_in_flight("acc-1"), 2);

    let limiter_clone = Arc::clone(&limiter);
    let waiter = tokio::spawn(async move {
        limiter_clone
            .acquire_bounded("acc-1", 2, Duration::from_secs(2))
            .await
    });

    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        !waiter.is_finished(),
        "3rd request should be queued while 2 slots are held"
    );

    drop(g1);
    let g3 = waiter
        .await
        .expect("waiter task should not panic")
        .expect("3rd request should be woken and acquire slot after g1 drop");
    assert_eq!(limiter.get_in_flight("acc-1"), 2);

    drop(g2);
    drop(g3);
    assert_eq!(limiter.get_in_flight("acc-1"), 0);
}

#[tokio::test]
async fn test_account_limiter_times_out_when_slots_held() {
    let limiter = AccountConcurrencyLimiter::new();
    let g1 = limiter
        .acquire_bounded("acc-timeout", 2, Duration::from_millis(200))
        .await
        .unwrap();
    let g2 = limiter
        .acquire_bounded("acc-timeout", 2, Duration::from_millis(200))
        .await
        .unwrap();

    let start = std::time::Instant::now();
    let res = limiter
        .acquire_bounded("acc-timeout", 2, Duration::from_millis(60))
        .await;
    let elapsed = start.elapsed();

    assert!(
        res.is_err(),
        "expected timeout error when all slots remain held"
    );
    let err = res.unwrap_err();
    assert!(
        err.contains("concurrency limit (2) reached"),
        "unexpected error message: {}",
        err
    );
    assert!(elapsed >= Duration::from_millis(45));

    drop(g1);
    drop(g2);
    assert_eq!(limiter.get_in_flight("acc-timeout"), 0);
}

#[tokio::test]
async fn test_session_bindings_ttl_and_capacity_eviction() {
    let cap_map = SessionBindings::with_limits(3, Duration::from_secs(60));
    assert_eq!(cap_map.bind_or_get("s1", "a1"), "a1");
    assert_eq!(cap_map.bind_or_get("s2", "a2"), "a2");
    assert_eq!(cap_map.bind_or_get("s3", "a3"), "a3");
    assert_eq!(cap_map.len(), 3);

    assert_eq!(cap_map.bind_or_get("s4", "a4"), "a4");
    assert!(cap_map.len() <= 3, "capacity must not exceed max_entries=3");
    assert_eq!(cap_map.get("s4").as_deref(), Some("a4"));

    let ttl_map = SessionBindings::with_limits(10, Duration::from_millis(40));
    assert_eq!(ttl_map.bind_or_get("s_ttl", "acc-x"), "acc-x");
    assert_eq!(ttl_map.get("s_ttl").as_deref(), Some("acc-x"));

    tokio::time::sleep(Duration::from_millis(60)).await;
    assert_eq!(ttl_map.evict_expired(), 1);
    assert!(ttl_map.get("s_ttl").is_none());
}

#[tokio::test]
async fn test_get_token_by_email_triggers_refresh_when_expired() {
    let manager = TokenManager::new(std::env::temp_dir());
    let now = chrono::Utc::now().timestamp();

    // Expired 10s ago, expires_in = 3600: must NOT skip refresh due to double-adding expires_in
    let expired_tok = make_test_token("acc-exp", "expired@test.com", now - 10, 3600);
    manager.insert_token_for_test(expired_tok);

    let res = manager.get_token_by_email("expired@test.com").await;
    assert!(
        res.is_err(),
        "expired token must enter refresh path instead of returning stale token"
    );

    // Valid for another 600s (> 300s buffer): should return cached token without refresh
    let fresh_tok = make_test_token("acc-fresh", "fresh@test.com", now + 600, 3600);
    manager.insert_token_for_test(fresh_tok);

    let ok_res = manager.get_token_by_email("fresh@test.com").await;
    assert!(ok_res.is_ok(), "fresh token should be returned from cache");
    assert_eq!(ok_res.unwrap().0, "stale_access_token");
}
