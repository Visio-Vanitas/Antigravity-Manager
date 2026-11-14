use once_cell::sync::Lazy;
use parking_lot::RwLock;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::SystemTime;

use crate::models::AppConfig;

/// In-memory snapshot of `AppConfig` keyed by file path and filesystem metadata.
///
/// Storing `(path, len, modified)` alongside the parsed `AppConfig` avoids
/// repeated disk reads, JSON deserialization, and migration checks on hot paths
/// while staying transparent to:
/// - External manual edits to `gui_config.json`
/// - `ABV_DATA_DIR` environment variable switches
/// - Isolated temporary directories used by unit tests
struct CachedAppConfig {
    path: PathBuf,
    len: u64,
    modified: Option<SystemTime>,
    config: AppConfig,
}

static CONFIG_CACHE: Lazy<RwLock<Option<CachedAppConfig>>> = Lazy::new(|| RwLock::new(None));

static CACHE_HITS: AtomicU64 = AtomicU64::new(0);
static CACHE_MISSES: AtomicU64 = AtomicU64::new(0);

#[inline]
fn read_file_stamp(path: &Path) -> Option<(u64, Option<SystemTime>)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().ok()))
}

/// Try to load `AppConfig` from the process-wide in-memory cache.
///
/// Returns `Some(AppConfig)` only when `path` exists and its `(len, modified)`
/// metadata matches the cached entry; otherwise increments the miss counter and
/// returns `None`.
pub fn try_get_cached(path: &Path) -> Option<AppConfig> {
    let Some((len, modified)) = read_file_stamp(path) else {
        CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
        return None;
    };

    let guard = CONFIG_CACHE.read();
    if let Some(cached) = guard.as_ref() {
        if cached.path == path && cached.len == len && cached.modified == modified {
            CACHE_HITS.fetch_add(1, Ordering::Relaxed);
            return Some(cached.config.clone());
        }
    }

    CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    None
}

/// Write-through refresh of the in-memory `AppConfig` cache.
///
/// Reads current filesystem metadata for `path` and stores `config.clone()`.
/// If metadata cannot be read (e.g. file removed), the cache slot is cleared
/// so stale state is never served.
pub fn store_cached(path: &Path, config: &AppConfig) {
    let stamp = read_file_stamp(path);
    let mut guard = CONFIG_CACHE.write();
    match stamp {
        Some((len, modified)) => {
            *guard = Some(CachedAppConfig {
                path: path.to_path_buf(),
                len,
                modified,
                config: config.clone(),
            });
        }
        None => {
            *guard = None;
        }
    }
}

/// Clear the in-memory `AppConfig` cache slot.
pub fn invalidate_config_cache() {
    let mut guard = CONFIG_CACHE.write();
    *guard = None;
}

/// Return `(hits, misses)` atomic counters for diagnostics and unit tests.
pub fn cache_stats() -> (u64, u64) {
    (
        CACHE_HITS.load(Ordering::Relaxed),
        CACHE_MISSES.load(Ordering::Relaxed),
    )
}
