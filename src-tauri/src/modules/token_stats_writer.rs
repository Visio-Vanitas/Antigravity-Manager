//! TokenStats 单写者异步批写后台通道

use crate::modules::token_stats::{get_db_path, open_conn_at};
use rusqlite::{params, Connection, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::OnceLock;
use std::time::Duration;

pub struct TokenUsageEvent {
    pub db_path: PathBuf,
    pub timestamp: i64,
    pub hour_bucket: String,
    pub account_email: String,
    pub model: String,
    pub requested_model: Option<String>,
    pub input_tokens: u32,
    pub output_tokens: u32,
    pub cached_tokens: u32,
    pub reasoning_tokens: u32,
    pub ttft_ms: Option<f64>,
}

enum StatsWriterMsg {
    Event(TokenUsageEvent),
    Flush(SyncSender<()>),
}

static WRITER_TX: OnceLock<SyncSender<StatsWriterMsg>> = OnceLock::new();

pub fn enqueue_usage(
    account_email: &str,
    model: &str,
    requested_model: Option<&str>,
    input_tokens: u32,
    output_tokens: u32,
    cached_tokens: u32,
    reasoning_tokens: u32,
    ttft_ms: Option<f64>,
) -> Result<(), String> {
    let db_path = get_db_path()?;
    let now = chrono::Local::now();
    let event = TokenUsageEvent {
        db_path,
        timestamp: now.timestamp(),
        hour_bucket: now.format("%Y-%m-%d %H:00").to_string(),
        account_email: account_email.to_string(),
        model: model.to_string(),
        requested_model: requested_model.map(|s| s.to_string()),
        input_tokens,
        output_tokens,
        cached_tokens,
        reasoning_tokens,
        ttft_ms,
    };

    let tx = WRITER_TX.get_or_init(spawn_writer);
    match tx.try_send(StatsWriterMsg::Event(event)) {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            tracing::warn!("[token_stats_writer] Writer queue full, dropping stats event");
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err("Token stats writer disconnected".to_string()),
    }
}

pub fn flush_for_test() {
    if let Some(tx) = WRITER_TX.get() {
        let (ack_tx, ack_rx) = sync_channel(1);
        if tx.send(StatsWriterMsg::Flush(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
    }
}

fn spawn_writer() -> SyncSender<StatsWriterMsg> {
    let (tx, rx) = sync_channel::<StatsWriterMsg>(4096);
    let _ = std::thread::Builder::new()
        .name("token-stats-writer".to_string())
        .spawn(move || {
            let mut conn_state: Option<(PathBuf, Connection)> = None;
            let mut batch: Vec<TokenUsageEvent> = Vec::with_capacity(64);
            loop {
                let msg = if batch.is_empty() {
                    rx.recv().map_err(|_| RecvTimeoutError::Disconnected)
                } else {
                    rx.recv_timeout(Duration::from_millis(150))
                };
                match msg {
                    Ok(StatsWriterMsg::Event(ev)) => {
                        batch.push(ev);
                        while batch.len() < 64 {
                            match rx.try_recv() {
                                Ok(StatsWriterMsg::Event(more)) => batch.push(more),
                                Ok(StatsWriterMsg::Flush(ack)) => {
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
                    Ok(StatsWriterMsg::Flush(ack)) => {
                        while let Ok(StatsWriterMsg::Event(more)) = rx.try_recv() {
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

fn flush_batch(conn_state: &mut Option<(PathBuf, Connection)>, batch: &mut Vec<TokenUsageEvent>) {
    if batch.is_empty() {
        return;
    }
    let mut start = 0;
    while start < batch.len() {
        let path = &batch[start].db_path;
        let end = batch[start..]
            .iter()
            .position(|e| &e.db_path != path)
            .map(|p| start + p)
            .unwrap_or(batch.len());
        if let Err(e) = write_slice(conn_state, path, &batch[start..end]) {
            tracing::error!("[token_stats_writer] Batch write failed: {}", e);
        }
        start = end;
    }
    batch.clear();
}

fn write_slice(
    conn_state: &mut Option<(PathBuf, Connection)>,
    db_path: &Path,
    events: &[TokenUsageEvent],
) -> Result<(), String> {
    let need_reopen = !matches!(conn_state, Some((p, _)) if p.as_path() == db_path);
    if need_reopen {
        let conn = open_conn_at(db_path)?;
        *conn_state = Some((db_path.to_path_buf(), conn));
    }
    let conn = &mut conn_state.as_mut().unwrap().1;
    let tx = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| e.to_string())?;
    for ev in events {
        let total_tokens = ev.input_tokens + ev.output_tokens;
        tx.execute(
            "INSERT INTO token_usage (timestamp, account_email, model, requested_model, input_tokens, output_tokens, cached_tokens, reasoning_tokens, total_tokens, ttft_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![ev.timestamp, ev.account_email, ev.model, ev.requested_model, ev.input_tokens, ev.output_tokens, ev.cached_tokens, ev.reasoning_tokens, total_tokens, ev.ttft_ms],
        ).map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT INTO token_stats_hourly (hour_bucket, account_email, total_input_tokens, total_output_tokens, total_cached_tokens, total_reasoning_tokens, total_tokens, request_count)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1)
             ON CONFLICT(hour_bucket, account_email) DO UPDATE SET
                total_input_tokens = total_input_tokens + ?3,
                total_output_tokens = total_output_tokens + ?4,
                total_cached_tokens = total_cached_tokens + ?5,
                total_reasoning_tokens = total_reasoning_tokens + ?6,
                total_tokens = total_tokens + ?7,
                request_count = request_count + 1",
            params![ev.hour_bucket, ev.account_email, ev.input_tokens, ev.output_tokens, ev.cached_tokens, ev.reasoning_tokens, total_tokens],
        ).map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())
}
