//! UserToken 内存读缓存与异步批写模块

use crate::modules::user_token_db::{
    check_token_basic_validity, get_db_path, load_token_by_value_from_db,
    load_token_ip_set_from_db, open_conn_at, UserToken,
};
use rusqlite::{params, Connection, TransactionBehavior};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{OnceLock, RwLock};
use std::time::Duration;
use uuid::Uuid;

struct CacheState {
    db_path: PathBuf,
    tokens_by_value: HashMap<String, Option<UserToken>>,
    ip_bindings: HashMap<String, HashSet<String>>,
}
static CACHE: RwLock<Option<CacheState>> = RwLock::new(None);
static CACHE_EPOCH: AtomicU64 = AtomicU64::new(0);
pub struct UsageEvent {
    pub db_path: PathBuf,
    pub token_id: String,
    pub ip: String,
    pub model: String,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub status: u16,
    pub user_agent: Option<String>,
    pub timestamp: i64,
}
enum WriterMsg {
    Event(UsageEvent),
    Flush(SyncSender<()>),
}
static WRITER_TX: OnceLock<SyncSender<WriterMsg>> = OnceLock::new();

pub fn invalidate() {
    if let Ok(mut guard) = CACHE.write() {
        CACHE_EPOCH.fetch_add(1, Ordering::AcqRel);
        *guard = None;
    }
}

fn ensure_state<'a>(guard: &'a mut Option<CacheState>, db_path: PathBuf) -> &'a mut CacheState {
    if !matches!(guard.as_ref(), Some(s) if s.db_path == db_path) {
        *guard = Some(CacheState {
            db_path,
            tokens_by_value: HashMap::new(),
            ip_bindings: HashMap::new(),
        });
    }
    guard.as_mut().unwrap()
}

pub fn get_token_by_value(token_str: &str) -> Result<Option<UserToken>, String> {
    let db_path = get_db_path()?;
    if let Ok(guard) = CACHE.read() {
        if let Some(cached) = guard
            .as_ref()
            .filter(|s| s.db_path == db_path)
            .and_then(|s| s.tokens_by_value.get(token_str))
        {
            return Ok(cached.clone());
        }
    }
    let epoch_before = CACHE_EPOCH.load(Ordering::Acquire);
    let loaded = load_token_by_value_from_db(token_str)?;
    if let Ok(mut guard) = CACHE.write() {
        if CACHE_EPOCH.load(Ordering::Acquire) == epoch_before {
            ensure_state(&mut guard, db_path)
                .tokens_by_value
                .insert(token_str.to_string(), loaded.clone());
        }
    }
    Ok(loaded)
}

pub fn validate_token(token_str: &str, ip: &str) -> Result<(bool, Option<String>), String> {
    let Some(token) = get_token_by_value(token_str)? else {
        return Ok((
            false,
            Some("Invalid token. Please check your API key.".to_string()),
        ));
    };
    if let Some(reason) = check_token_basic_validity(&token) {
        return Ok((false, Some(reason)));
    }
    if token.max_ips > 0 {
        let db_path = get_db_path()?;
        let mut need_load = true;
        if let Ok(guard) = CACHE.read() {
            if let Some(ips) = guard
                .as_ref()
                .filter(|s| s.db_path == db_path)
                .and_then(|s| s.ip_bindings.get(&token.id))
            {
                if ips.contains(ip) {
                    return Ok((true, None));
                }
                if ips.len() >= token.max_ips as usize {
                    return Ok((false, Some(ip_limit_msg(ips.len(), token.max_ips))));
                }
                need_load = false;
            }
        }
        let loaded_ips = if need_load {
            Some(load_token_ip_set_from_db(&token.id)?)
        } else {
            None
        };
        if let Ok(mut guard) = CACHE.write() {
            let ips = ensure_state(&mut guard, db_path)
                .ip_bindings
                .entry(token.id.clone())
                .or_insert_with(|| loaded_ips.unwrap_or_default());
            if !ips.contains(ip) {
                if ips.len() >= token.max_ips as usize {
                    return Ok((false, Some(ip_limit_msg(ips.len(), token.max_ips))));
                }
                ips.insert(ip.to_string());
            }
        }
    }
    Ok((true, None))
}

fn ip_limit_msg(current: usize, max_ips: i32) -> String {
    format!(
        "IP limit reached ({}/{}). Please contact the administrator to increase the limit.",
        current, max_ips
    )
}

pub fn enqueue_usage_event(
    token_id: &str,
    ip: &str,
    model: &str,
    input_tokens: i32,
    output_tokens: i32,
    status: u16,
    user_agent: Option<String>,
) -> Result<(), String> {
    let event = UsageEvent {
        db_path: get_db_path()?,
        token_id: token_id.to_string(),
        ip: ip.to_string(),
        model: model.to_string(),
        input_tokens,
        output_tokens,
        status,
        user_agent,
        timestamp: chrono::Utc::now().timestamp(),
    };
    match WRITER_TX
        .get_or_init(spawn_writer)
        .try_send(WriterMsg::Event(event))
    {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            tracing::warn!("[user_token_cache] Usage writer queue full, dropping usage log");
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err("User token writer disconnected".to_string()),
    }
}

pub fn flush_for_test() {
    if let Some(tx) = WRITER_TX.get() {
        let (ack_tx, ack_rx) = sync_channel(1);
        if tx.send(WriterMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }
}

fn spawn_writer() -> SyncSender<WriterMsg> {
    let (tx, rx) = sync_channel::<WriterMsg>(4096);
    let _ = std::thread::Builder::new()
        .name("user-token-writer".to_string())
        .spawn(move || {
            let mut conn_state: Option<(PathBuf, Connection)> = None;
            let mut batch: Vec<UsageEvent> = Vec::with_capacity(64);
            loop {
                let msg = if batch.is_empty() {
                    rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
                } else {
                    rx.recv_timeout(Duration::from_millis(100))
                };
                match msg {
                    Ok(WriterMsg::Event(ev)) => {
                        batch.push(ev);
                        while batch.len() < 64 {
                            match rx.try_recv() {
                                Ok(WriterMsg::Event(more)) => batch.push(more),
                                Ok(WriterMsg::Flush(ack)) => {
                                    flush_batch(&mut conn_state, &mut batch);
                                    let _ = ack.send(());
                                }
                                Err(_) => break,
                            }
                        }
                        if batch.len() >= 64 {
                            flush_batch(&mut conn_state, &mut batch);
                        }
                    }
                    Ok(WriterMsg::Flush(ack)) => {
                        while let Ok(WriterMsg::Event(more)) = rx.try_recv() {
                            batch.push(more);
                        }
                        flush_batch(&mut conn_state, &mut batch);
                        let _ = ack.send(());
                    }
                    Err(RecvTimeoutError::Timeout) => flush_batch(&mut conn_state, &mut batch),
                    Err(RecvTimeoutError::Disconnected) => {
                        flush_batch(&mut conn_state, &mut batch);
                        break;
                    }
                }
            }
        });
    tx
}

fn flush_batch(conn_state: &mut Option<(PathBuf, Connection)>, batch: &mut Vec<UsageEvent>) {
    let mut start = 0;
    while start < batch.len() {
        let path = &batch[start].db_path;
        let end = batch[start..]
            .iter()
            .position(|e| &e.db_path != path)
            .map(|p| start + p)
            .unwrap_or(batch.len());
        if let Err(e) = write_slice(conn_state, path, &batch[start..end]) {
            tracing::error!("[user_token_cache] Batch write failed: {}", e);
        }
        start = end;
    }
    batch.clear();
}

fn write_slice(
    conn_state: &mut Option<(PathBuf, Connection)>,
    db_path: &Path,
    events: &[UsageEvent],
) -> Result<(), String> {
    if !matches!(conn_state, Some((p, _)) if p.as_path() == db_path) {
        *conn_state = Some((db_path.to_path_buf(), open_conn_at(db_path)?));
    }
    let tx = conn_state
        .as_mut()
        .unwrap()
        .1
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| e.to_string())?;
    for ev in events {
        tx.execute("UPDATE user_tokens SET last_used_at = ?1, total_requests = total_requests + 1, total_tokens_used = total_tokens_used + ?2 WHERE id = ?3",
            params![ev.timestamp, ev.input_tokens + ev.output_tokens, ev.token_id]).map_err(|e| e.to_string())?;
        tx.execute("INSERT INTO token_ip_bindings (id, token_id, ip_address, first_seen_at, last_seen_at, request_count, user_agent) VALUES (?1, ?2, ?3, ?4, ?4, 1, ?5)
             ON CONFLICT(token_id, ip_address) DO UPDATE SET last_seen_at = excluded.last_seen_at, request_count = request_count + 1, user_agent = COALESCE(excluded.user_agent, token_ip_bindings.user_agent)",
            params![Uuid::new_v4().to_string(), ev.token_id, ev.ip, ev.timestamp, ev.user_agent]).map_err(|e| e.to_string())?;
        tx.execute("INSERT INTO token_usage_logs (id, token_id, ip_address, model, input_tokens, output_tokens, request_time, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![Uuid::new_v4().to_string(), ev.token_id, ev.ip, ev.model, ev.input_tokens, ev.output_tokens, ev.timestamp, ev.status]).map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())
}
