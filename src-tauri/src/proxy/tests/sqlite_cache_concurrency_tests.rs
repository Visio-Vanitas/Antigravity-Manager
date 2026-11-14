//! SQLite 内存缓存与异步批写并发测试

#[cfg(test)]
mod tests {
    use crate::modules::{
        security_cache, security_db, token_stats, token_stats_writer, user_token_cache,
        user_token_db,
    };
    use crate::proxy::monitor::prompt_log_tests::TestDataDir;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_user_token_concurrent_ip_limit_and_batch_flush() {
        let _dir = TestDataDir::new();
        user_token_db::init_db().unwrap();

        let tok = user_token_db::create_token(
            "concurrent_user".to_string(),
            "never".to_string(),
            Some("concurrency test".to_string()),
            2,
            None,
            None,
            None,
        )
        .unwrap();

        let allowed = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::with_capacity(32);

        // 先并发发起 24 个来自前 2 个 IP 的请求，确保前 2 个 IP 抢占名额
        for i in 0..24 {
            let token_val = tok.token.clone();
            let token_id = tok.id.clone();
            let ip = if i % 2 == 0 { "10.0.0.1" } else { "10.0.0.2" };
            let allowed = Arc::clone(&allowed);
            let rejected = Arc::clone(&rejected);
            handles.push(tokio::spawn(async move {
                let (valid, _) = user_token_db::validate_token(&token_val, ip).unwrap();
                if valid {
                    allowed.fetch_add(1, Ordering::Relaxed);
                    user_token_db::record_token_usage_and_ip(
                        &token_id,
                        ip,
                        "gemini-2.5-flash",
                        10,
                        20,
                        200,
                        None,
                    )
                    .unwrap();
                } else {
                    rejected.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles.drain(..) {
            h.await.unwrap();
        }

        // 再并发发起 8 个第 3 个 IP 的请求，必须全部被 max_ips=2 稳定拦截
        for _ in 0..8 {
            let token_val = tok.token.clone();
            let allowed = Arc::clone(&allowed);
            let rejected = Arc::clone(&rejected);
            handles.push(tokio::spawn(async move {
                let (valid, reason) =
                    user_token_db::validate_token(&token_val, "10.0.0.3").unwrap();
                if valid {
                    allowed.fetch_add(1, Ordering::Relaxed);
                } else {
                    assert!(reason.unwrap_or_default().contains("IP limit reached"));
                    rejected.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(allowed.load(Ordering::Relaxed), 24);
        assert_eq!(rejected.load(Ordering::Relaxed), 8);

        user_token_cache::flush_for_test();

        let persisted = user_token_db::get_token_by_id(&tok.id).unwrap().unwrap();
        assert_eq!(persisted.total_requests, 24);
        assert_eq!(persisted.total_tokens_used, 24 * 30);

        let stats = user_token_db::get_token_detailed_stats(&tok.id, "7d").unwrap();
        assert_eq!(stats.summary.total_requests, 24);
        let ips = user_token_db::get_token_ips(&tok.id).unwrap();
        assert_eq!(ips.len(), 2);
    }

    #[tokio::test]
    async fn test_security_cache_cidr_expiry_invalidation_and_log_flush() {
        let _dir = TestDataDir::new();
        security_db::init_db().unwrap();

        security_db::add_to_whitelist("10.1.0.0/16", Some("lan")).unwrap();
        let bl_cidr =
            security_db::add_to_blacklist("192.168.50.0/24", Some("subnet"), None, "test").unwrap();
        let expired_at = chrono::Utc::now().timestamp() - 60;
        security_db::add_to_blacklist("192.168.60.1", Some("expired"), Some(expired_at), "test")
            .unwrap();

        assert!(security_db::is_ip_in_whitelist("10.1.2.3").unwrap());
        assert!(!security_db::is_ip_in_whitelist("10.2.0.1").unwrap());
        assert!(security_db::is_ip_in_blacklist("192.168.50.99").unwrap());
        assert!(!security_db::is_ip_in_blacklist("192.168.60.1").unwrap());

        // 写穿失效：删除黑名单后内存缓存立即失效
        security_db::remove_from_blacklist(&bl_cidr.id).unwrap();
        assert!(!security_db::is_ip_in_blacklist("192.168.50.99").unwrap());

        // 并发异步写入 50 条访问日志并刷盘验证
        let mut handles = Vec::with_capacity(50);
        for i in 0..50 {
            handles.push(tokio::spawn(async move {
                let log = security_db::IpAccessLog {
                    id: uuid::Uuid::new_v4().to_string(),
                    client_ip: format!("172.16.0.{}", i % 10),
                    timestamp: chrono::Utc::now().timestamp(),
                    method: Some("POST".to_string()),
                    path: Some("/v1/chat/completions".to_string()),
                    user_agent: None,
                    status: Some(200),
                    duration: Some(15),
                    api_key_hash: None,
                    blocked: false,
                    block_reason: None,
                    username: None,
                };
                security_db::save_ip_access_log_async(log).unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        security_cache::flush_for_test();
        assert_eq!(
            security_db::get_ip_access_logs_count(None, false).unwrap(),
            50
        );
    }

    #[tokio::test]
    async fn test_token_stats_concurrent_async_flush() {
        let _dir = TestDataDir::new();
        token_stats::init_db().unwrap();

        let mut handles = Vec::with_capacity(100);
        for _ in 0..100 {
            handles.push(tokio::spawn(async move {
                token_stats::record_usage_async(
                    "acc@test.com",
                    "gemini-2.5-pro",
                    Some("claude-sonnet"),
                    10,
                    20,
                    5,
                    2,
                    Some(100.0),
                )
                .unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        token_stats_writer::flush_for_test();

        let summary = token_stats::get_summary_stats(24).unwrap();
        assert_eq!(summary.total_requests, 100);
        assert_eq!(summary.total_input_tokens, 1000);
        assert_eq!(summary.total_output_tokens, 2000);
        assert_eq!(summary.total_cached_tokens, 500);
        assert_eq!(summary.total_reasoning_tokens, 200);
        assert_eq!(summary.total_tokens, 3000);
    }
}
