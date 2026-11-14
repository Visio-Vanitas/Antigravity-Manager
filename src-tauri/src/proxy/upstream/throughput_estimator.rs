//! 按出口键隔离的被动上行吞吐 EWMA 估计器
//!
//! 仅在真实请求上传产生进展后被动更新，不发起任何主动测速请求。
//! 将高速直连出口与慢速代理出口（如 VLESS Reality）的上行速率估计彻底隔离，
//! 防止快链路的样本污染慢链路的上传超时预算。

use dashmap::DashMap;
use std::sync::LazyLock;
use std::time::Duration;

/// 默认出口桶键（未命中代理池或未指定出口时使用）
pub const DEFAULT_EGRESS_KEY: &str = "default";

/// 合法被动测速样本下限（4 KB/s）与上限（1 GB/s）
const MIN_SAMPLE_BPS: u64 = 4 * 1024;
const MAX_SAMPLE_BPS: u64 = 1024 * 1024 * 1024;

/// 有效采样最短耗时门槛（秒），过滤小包被 socket/TLS 缓冲瞬间吞掉的虚高样本
const MIN_SAMPLE_ELAPSED_SECS: f64 = 0.5;

/// 分桶容量上限，防止极端动态 URL 导致内存无界增长
const MAX_EGRESS_BUCKETS: usize = 512;

/// 按出口键分桶的被动上行吞吐 EWMA（bytes/s）
static UPLOAD_THROUGHPUT_BY_EGRESS: LazyLock<DashMap<String, f64>> = LazyLock::new(DashMap::new);

#[inline]
fn normalize_egress_key(egress_key: &str) -> &str {
    let trimmed = egress_key.trim();
    if trimmed.is_empty() {
        DEFAULT_EGRESS_KEY
    } else {
        trimmed
    }
}

/// 读取指定出口键的当前被动上行速度估计（bytes/s）
pub fn current_upload_throughput_bps_for(egress_key: &str) -> Option<u64> {
    UPLOAD_THROUGHPUT_BY_EGRESS
        .get(normalize_egress_key(egress_key))
        .map(|v| *v as u64)
}

/// 用一个上传样本更新指定出口键的 EWMA（样本过小或过快时直接忽略）
pub fn observe_upload_throughput_for(egress_key: &str, bps: u64) {
    if !(MIN_SAMPLE_BPS..=MAX_SAMPLE_BPS).contains(&bps) {
        return;
    }

    let key = normalize_egress_key(egress_key);
    if !UPLOAD_THROUGHPUT_BY_EGRESS.contains_key(key)
        && UPLOAD_THROUGHPUT_BY_EGRESS.len() >= MAX_EGRESS_BUCKETS
    {
        UPLOAD_THROUGHPUT_BY_EGRESS.clear();
    }

    UPLOAD_THROUGHPUT_BY_EGRESS
        .entry(key.to_string())
        .and_modify(|prev| {
            *prev = *prev * 0.7 + (bps as f64) * 0.3;
        })
        .or_insert(bps as f64);
}

/// 记录指定出口键的一次上传耗时样本
pub fn record_upload_throughput_sample_for(egress_key: &str, bytes: usize, elapsed: Duration) {
    let secs = elapsed.as_secs_f64();
    if bytes == 0 || secs < MIN_SAMPLE_ELAPSED_SECS {
        return;
    }
    observe_upload_throughput_for(egress_key, (bytes as f64 / secs) as u64);
}

/// 读取默认出口桶的当前被动上行速度估计（bytes/s，向后兼容 API）
pub fn current_upload_throughput_bps() -> Option<u64> {
    current_upload_throughput_bps_for(DEFAULT_EGRESS_KEY)
}

/// 更新默认出口桶的 EWMA（向后兼容 API）
pub fn observe_upload_throughput(bps: u64) {
    observe_upload_throughput_for(DEFAULT_EGRESS_KEY, bps);
}

/// 清空全部出口桶的被动上行速度估计（单元测试隔离与运行时重置）
pub fn reset_upload_throughput_estimate() {
    UPLOAD_THROUGHPUT_BY_EGRESS.clear();
}

/// 记录默认出口桶的一次上传耗时样本（向后兼容 API）
pub fn record_upload_throughput_sample(bytes: usize, elapsed: Duration) {
    record_upload_throughput_sample_for(DEFAULT_EGRESS_KEY, bytes, elapsed);
}

/// 返回当前活跃的出口分桶数量（诊断/测试用途）
pub fn egress_bucket_count() -> usize {
    UPLOAD_THROUGHPUT_BY_EGRESS.len()
}
