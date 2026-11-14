#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;

    use crate::models::AppConfig;
    use crate::modules::account::get_data_dir;
    use crate::modules::config::{load_app_config, save_app_config};
    use crate::modules::config_cache::{cache_stats, invalidate_config_cache};
    use crate::proxy::monitor::prompt_log_tests::TestDataDir;

    #[test]
    fn test_repeated_load_hits_memory_cache() {
        let _dir = TestDataDir::new();
        invalidate_config_cache();

        let (hits_before, misses_before) = cache_stats();
        let initial = load_app_config().expect("initial load should succeed");
        let (hits_after_first, misses_after_first) = cache_stats();

        assert_eq!(hits_after_first, hits_before);
        assert_eq!(misses_after_first, misses_before + 1);

        for _ in 0..5 {
            let loaded = load_app_config().expect("cached load should succeed");
            assert_eq!(loaded.proxy.api_key, initial.proxy.api_key);
            assert_eq!(loaded.proxy.port, initial.proxy.port);
        }

        let (hits_after_loop, misses_after_loop) = cache_stats();
        assert_eq!(hits_after_loop, hits_after_first + 5);
        assert_eq!(misses_after_loop, misses_after_first);
    }

    #[test]
    fn test_save_app_config_write_through_updates_cache() {
        let _dir = TestDataDir::new();
        invalidate_config_cache();

        let mut cfg = load_app_config().expect("initial load should succeed");
        cfg.proxy.api_key = "sk-write-through-test-key".to_string();
        cfg.proxy.port = 18080;
        cfg.refresh_interval = 42;

        save_app_config(&cfg).expect("save_app_config should succeed");

        let (hits_before, misses_before) = cache_stats();
        let reloaded = load_app_config().expect("load after save should succeed");
        let (hits_after, misses_after) = cache_stats();

        assert_eq!(reloaded.proxy.api_key, "sk-write-through-test-key");
        assert_eq!(reloaded.proxy.port, 18080);
        assert_eq!(reloaded.refresh_interval, 42);
        assert_eq!(hits_after, hits_before + 1);
        assert_eq!(misses_after, misses_before);
    }

    #[test]
    fn test_external_disk_modification_invalidates_cache() {
        let _dir = TestDataDir::new();
        invalidate_config_cache();

        let mut cfg = load_app_config().expect("initial load should succeed");
        assert_ne!(cfg.proxy.api_key, "sk-externally-modified-longer-key-value");

        // Simulate an external editor modifying gui_config.json on disk directly
        let config_path = get_data_dir()
            .expect("get_data_dir should succeed")
            .join("gui_config.json");
        cfg.proxy.api_key = "sk-externally-modified-longer-key-value".to_string();
        cfg.proxy.port = 19123;
        let json = serde_json::to_string_pretty(&cfg).expect("serialize should succeed");
        fs::write(&config_path, json.as_bytes()).expect("external write should succeed");

        let (hits_before, misses_before) = cache_stats();
        let reloaded = load_app_config().expect("load after external edit should succeed");
        let (hits_after, misses_after) = cache_stats();

        assert_eq!(
            reloaded.proxy.api_key,
            "sk-externally-modified-longer-key-value"
        );
        assert_eq!(reloaded.proxy.port, 19123);
        assert_eq!(hits_after, hits_before);
        assert_eq!(misses_after, misses_before + 1);

        // Subsequent load hits cache again with the updated config
        let cached_again = load_app_config().expect("subsequent load should hit cache");
        assert_eq!(
            cached_again.proxy.api_key,
            "sk-externally-modified-longer-key-value"
        );
        let (hits_final, misses_final) = cache_stats();
        assert_eq!(hits_final, hits_after + 1);
        assert_eq!(misses_final, misses_after);
    }

    #[test]
    fn test_concurrent_multithreaded_load_app_config_consistency() {
        let _dir = TestDataDir::new();
        invalidate_config_cache();

        let mut seed = AppConfig::new();
        seed.proxy.api_key = "sk-concurrent-seed-key".to_string();
        seed.proxy.port = 18456;
        seed.language = "en".to_string();
        save_app_config(&seed).expect("seed save should succeed");

        const THREADS: usize = 16;
        const ITERATIONS: usize = 50;
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let b = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                b.wait();
                for _ in 0..ITERATIONS {
                    let cfg = load_app_config().expect("concurrent load should succeed");
                    assert_eq!(cfg.proxy.api_key, "sk-concurrent-seed-key");
                    assert_eq!(cfg.proxy.port, 18456);
                    assert_eq!(cfg.language, "en");
                }
            }));
        }

        for h in handles {
            h.join().expect("worker thread should not panic");
        }
    }
}
