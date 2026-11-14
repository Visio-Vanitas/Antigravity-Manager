//! Security 黑白名单内存快照缓存与异步日志批写模块

use crate::modules::security_db::{
    get_security_db_path, load_blacklist_from_db, load_whitelist_from_db, open_conn_at,
    IpAccessLog, IpBlacklistEntry,
};
use rusqlite::{params, Connection, TransactionBehavior};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

enum ParsedCidr {
    V4 { network: u32, mask: u32 },
    V6 { network: u128, mask: u128 },
}
impl ParsedCidr {
    fn parse(cidr: &str) -> Option<Self> {
        let (net_str, prefix_str) = cidr.split_once('/')?;
        let prefix: u8 = prefix_str.trim().parse().ok()?;
        match normalize_ip(parse_ip_str(net_str)?) {
            IpAddr::V4(v4) if prefix <= 32 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    !0u32 << (32 - prefix)
                };
                Some(Self::V4 {
                    network: u32::from_be_bytes(v4.octets()) & mask,
                    mask,
                })
            }
            IpAddr::V6(v6) if prefix <= 128 => {
                let mask = if prefix == 0 {
                    0
                } else {
                    !0u128 << (128 - prefix)
                };
                Some(Self::V6 {
                    network: u128::from_be_bytes(v6.octets()) & mask,
                    mask,
                })
            }
            _ => None,
        }
    }
    fn matches(&self, addr: IpAddr) -> bool {
        match (self, normalize_ip(addr)) {
            (Self::V4 { network, mask }, IpAddr::V4(v4)) => {
                (u32::from_be_bytes(v4.octets()) & *mask) == *network
            }
            (Self::V6 { network, mask }, IpAddr::V6(v6)) => {
                (u128::from_be_bytes(v6.octets()) & *mask) == *network
            }
            _ => false,
        }
    }
}

struct CachedBlacklistEntry {
    entry: IpBlacklistEntry,
    exact_ip: Option<IpAddr>,
    cidr: Option<ParsedCidr>,
    hit_delta: AtomicI64,
}
struct SecuritySnapshot {
    db_path: PathBuf,
    whitelist_exact_str: HashSet<String>,
    whitelist_exact_ip: HashSet<IpAddr>,
    whitelist_cidrs: Vec<ParsedCidr>,
    blacklist: Vec<CachedBlacklistEntry>,
    blacklist_exact_str: HashMap<String, usize>,
}
static SNAPSHOT: RwLock<Option<Arc<SecuritySnapshot>>> = RwLock::new(None);
static SNAPSHOT_EPOCH: AtomicU64 = AtomicU64::new(0);
enum WriterMsg {
    Log { db_path: PathBuf, log: IpAccessLog },
    Flush(SyncSender<()>),
}
static WRITER_TX: OnceLock<SyncSender<WriterMsg>> = OnceLock::new();
fn parse_ip_str(s: &str) -> Option<IpAddr> {
    s.trim().trim_matches('[').trim_matches(']').parse().ok()
}
fn normalize_ip(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}
pub fn invalidate() {
    let old = SNAPSHOT.write().ok().and_then(|mut g| {
        SNAPSHOT_EPOCH.fetch_add(1, Ordering::AcqRel);
        g.take()
    });
    if let Some(snap) = old {
        let mut conn_state = None;
        flush_snapshot_hit_deltas(&snap, &mut conn_state);
    }
}

pub fn pending_hit_count(id: &str) -> i64 {
    let Ok(db_path) = get_security_db_path() else {
        return 0;
    };
    let Ok(guard) = SNAPSHOT.read() else { return 0 };
    guard
        .as_ref()
        .filter(|s| s.db_path == db_path)
        .and_then(|s| s.blacklist.iter().find(|e| e.entry.id == id))
        .map(|e| e.hit_delta.load(Ordering::Relaxed))
        .unwrap_or(0)
}

fn get_or_load_snapshot() -> Result<Arc<SecuritySnapshot>, String> {
    let db_path = get_security_db_path()?;
    for attempt in 0..4 {
        if let Ok(guard) = SNAPSHOT.read() {
            if let Some(snap) = guard.as_ref().filter(|s| s.db_path == db_path) {
                return Ok(Arc::clone(snap));
            }
        }
        let epoch_before = SNAPSHOT_EPOCH.load(Ordering::Acquire);
        let wl_entries = load_whitelist_from_db()?;
        let bl_entries = load_blacklist_from_db()?;
        let (mut whitelist_exact_str, mut whitelist_exact_ip, mut whitelist_cidrs) = (
            HashSet::with_capacity(wl_entries.len()),
            HashSet::new(),
            Vec::new(),
        );
        for e in wl_entries {
            if e.ip_pattern.contains('/') {
                if let Some(c) = ParsedCidr::parse(&e.ip_pattern) {
                    whitelist_cidrs.push(c);
                }
            } else {
                whitelist_exact_str.insert(e.ip_pattern.clone());
                if let Some(a) = parse_ip_str(&e.ip_pattern) {
                    whitelist_exact_ip.insert(normalize_ip(a));
                }
            }
        }
        let (mut blacklist, mut blacklist_exact_str) = (
            Vec::with_capacity(bl_entries.len()),
            HashMap::with_capacity(bl_entries.len()),
        );
        for (idx, entry) in bl_entries.into_iter().enumerate() {
            let cidr = if entry.ip_pattern.contains('/') {
                ParsedCidr::parse(&entry.ip_pattern)
            } else {
                blacklist_exact_str.insert(entry.ip_pattern.clone(), idx);
                None
            };
            let exact_ip = if cidr.is_none() {
                parse_ip_str(&entry.ip_pattern).map(normalize_ip)
            } else {
                None
            };
            blacklist.push(CachedBlacklistEntry {
                entry,
                exact_ip,
                cidr,
                hit_delta: AtomicI64::new(0),
            });
        }
        let snap = Arc::new(SecuritySnapshot {
            db_path: db_path.clone(),
            whitelist_exact_str,
            whitelist_exact_ip,
            whitelist_cidrs,
            blacklist,
            blacklist_exact_str,
        });
        if let Ok(mut guard) = SNAPSHOT.write() {
            if SNAPSHOT_EPOCH.load(Ordering::Acquire) == epoch_before {
                *guard = Some(Arc::clone(&snap));
                return Ok(snap);
            }
        }
        if attempt == 3 {
            return Ok(snap);
        }
    }
    unreachable!()
}

pub fn is_ip_in_whitelist(ip: &str) -> Result<bool, String> {
    let snap = get_or_load_snapshot()?;
    Ok(snap.whitelist_exact_str.contains(ip)
        || parse_ip_str(ip).map(normalize_ip).is_some_and(|a| {
            snap.whitelist_exact_ip.contains(&a)
                || snap.whitelist_cidrs.iter().any(|c| c.matches(a))
        }))
}

pub fn get_blacklist_entry_for_ip(ip: &str) -> Result<Option<IpBlacklistEntry>, String> {
    let snap = get_or_load_snapshot()?;
    let now = chrono::Utc::now().timestamp();
    let active = |i: &CachedBlacklistEntry| i.entry.expires_at.map_or(true, |exp| now < exp);
    if let Some(&idx) = snap.blacklist_exact_str.get(ip) {
        if active(&snap.blacklist[idx]) {
            return Ok(Some(record_hit(&snap.blacklist[idx])));
        }
    }
    if let Some(addr) = parse_ip_str(ip).map(normalize_ip) {
        if let Some(item) = snap.blacklist.iter().find(|i| {
            active(i)
                && (i.exact_ip == Some(addr) || i.cidr.as_ref().is_some_and(|c| c.matches(addr)))
        }) {
            return Ok(Some(record_hit(item)));
        }
    }
    Ok(None)
}

fn record_hit(item: &CachedBlacklistEntry) -> IpBlacklistEntry {
    let delta = item.hit_delta.fetch_add(1, Ordering::Relaxed) + 1;
    let _ = WRITER_TX.get_or_init(spawn_writer);
    IpBlacklistEntry {
        hit_count: item.entry.hit_count + delta,
        ..item.entry.clone()
    }
}

pub fn enqueue_access_log(log: IpAccessLog) -> Result<(), String> {
    let db_path = get_security_db_path()?;
    match WRITER_TX
        .get_or_init(spawn_writer)
        .try_send(WriterMsg::Log { db_path, log })
    {
        Ok(()) => Ok(()),
        Err(TrySendError::Full(_)) => {
            tracing::warn!("[security_cache] Access log queue full, dropping log");
            Ok(())
        }
        Err(TrySendError::Disconnected(_)) => Err("Security writer disconnected".to_string()),
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
        .name("security-log-writer".to_string())
        .spawn(move || {
            let (mut conn_state, mut batch) =
                (None::<(PathBuf, Connection)>, Vec::with_capacity(64));
            loop {
                match rx.recv_timeout(Duration::from_millis(200)) {
                    Ok(WriterMsg::Log { db_path, log }) => {
                        batch.push((db_path, log));
                        while batch.len() < 64 {
                            match rx.try_recv() {
                                Ok(WriterMsg::Log { db_path, log }) => batch.push((db_path, log)),
                                Ok(WriterMsg::Flush(ack)) => {
                                    flush_all(&mut conn_state, &mut batch);
                                    let _ = ack.send(());
                                }
                                Err(_) => break,
                            }
                        }
                        if batch.len() >= 64 {
                            flush_all(&mut conn_state, &mut batch);
                        }
                    }
                    Ok(WriterMsg::Flush(ack)) => {
                        while let Ok(WriterMsg::Log { db_path, log }) = rx.try_recv() {
                            batch.push((db_path, log));
                        }
                        flush_all(&mut conn_state, &mut batch);
                        let _ = ack.send(());
                    }
                    Err(RecvTimeoutError::Timeout) => flush_all(&mut conn_state, &mut batch),
                    Err(RecvTimeoutError::Disconnected) => {
                        flush_all(&mut conn_state, &mut batch);
                        break;
                    }
                }
            }
        });
    tx
}

fn flush_all(
    conn_state: &mut Option<(PathBuf, Connection)>,
    batch: &mut Vec<(PathBuf, IpAccessLog)>,
) {
    let mut start = 0;
    while start < batch.len() {
        let path = &batch[start].0;
        let end = batch[start..]
            .iter()
            .position(|(p, _)| p != path)
            .map(|p| start + p)
            .unwrap_or(batch.len());
        if let Err(e) = write_logs(conn_state, path, &batch[start..end]) {
            tracing::error!("[security_cache] Batch log write failed: {}", e);
        }
        start = end;
    }
    batch.clear();
    flush_hit_deltas(conn_state);
}

fn ensure_conn<'a>(
    conn_state: &'a mut Option<(PathBuf, Connection)>,
    db_path: &Path,
) -> Result<&'a mut Connection, String> {
    if !matches!(conn_state, Some((p, _)) if p.as_path() == db_path) {
        *conn_state = Some((db_path.to_path_buf(), open_conn_at(db_path)?));
    }
    Ok(&mut conn_state.as_mut().unwrap().1)
}
fn write_logs(
    conn_state: &mut Option<(PathBuf, Connection)>,
    db_path: &Path,
    logs: &[(PathBuf, IpAccessLog)],
) -> Result<(), String> {
    let tx = ensure_conn(conn_state, db_path)?
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|e| e.to_string())?;
    for (_, log) in logs {
        tx.execute("INSERT INTO ip_access_logs (id, client_ip, timestamp, method, path, user_agent, status, duration, api_key_hash, blocked, block_reason, username) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![log.id, log.client_ip, log.timestamp, log.method, log.path, log.user_agent, log.status, log.duration, log.api_key_hash, log.blocked, log.block_reason, log.username]).map_err(|e| e.to_string())?;
    }
    tx.commit().map_err(|e| e.to_string())
}
fn flush_hit_deltas(conn_state: &mut Option<(PathBuf, Connection)>) {
    let Some(snap) = SNAPSHOT
        .read()
        .ok()
        .and_then(|g| g.as_ref().map(Arc::clone))
    else {
        return;
    };
    flush_snapshot_hit_deltas(&snap, conn_state);
}

fn flush_snapshot_hit_deltas(
    snap: &SecuritySnapshot,
    conn_state: &mut Option<(PathBuf, Connection)>,
) {
    let updates: Vec<(&str, i64)> = snap
        .blacklist
        .iter()
        .filter_map(|i| {
            let d = i.hit_delta.swap(0, Ordering::AcqRel);
            (d > 0).then_some((i.entry.id.as_str(), d))
        })
        .collect();
    if updates.is_empty() {
        return;
    }
    if let Ok(tx) = ensure_conn(conn_state, &snap.db_path).and_then(|c| {
        c.transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| e.to_string())
    }) {
        for (id, delta) in updates {
            let _ = tx.execute(
                "UPDATE ip_blacklist SET hit_count = hit_count + ?1 WHERE id = ?2",
                params![delta, id],
            );
        }
        let _ = tx.commit();
    }
}
