use hyper::body::{Body, Incoming};
use hyper::service::Service;
use hyper::Request;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as AutoConnBuilder;
use std::error::Error;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

/// 默认最大并发入站活跃连接数（可通过环境变量 ANTIGRAVITY_MAX_CONNECTIONS 覆盖，0 表示不限制）
pub const DEFAULT_MAX_INBOUND_CONNECTIONS: usize = 4096;

/// 连接池满载时等待空闲槽位的宽限时间
const ACQUIRE_GRACE_TIMEOUT: Duration = Duration::from_millis(250);

/// 优雅关闭时等待存量连接排空的超时时间
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// 全局入站活跃连接准入闸门
#[derive(Clone)]
pub struct InboundConnectionLimiter {
    max_connections: usize,
    semaphore: Option<Arc<Semaphore>>,
    active_count: Arc<AtomicUsize>,
}

/// 活跃连接守卫，Drop 时自动归还并发配额并递减活跃计数
pub struct InboundConnectionGuard {
    _permit: Option<OwnedSemaphorePermit>,
    active_count: Arc<AtomicUsize>,
}

impl Drop for InboundConnectionGuard {
    fn drop(&mut self) {
        self.active_count.fetch_sub(1, Ordering::Relaxed);
    }
}

impl InboundConnectionLimiter {
    /// 从环境变量 `ANTIGRAVITY_MAX_CONNECTIONS` 初始化，未设置或无效时回退默认值
    pub fn from_env() -> Self {
        let max = std::env::var("ANTIGRAVITY_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_INBOUND_CONNECTIONS);
        Self::new(max)
    }

    /// 创建指定上限的入站连接准入闸门；`max_connections == 0` 表示不限制
    pub fn new(max_connections: usize) -> Self {
        let semaphore = if max_connections == 0 {
            None
        } else {
            Some(Arc::new(Semaphore::new(max_connections)))
        };
        Self {
            max_connections,
            semaphore,
            active_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// 返回当前实时活跃连接数
    pub fn active_connections(&self) -> usize {
        self.active_count.load(Ordering::Relaxed)
    }

    /// 返回配置的最大并发连接数（0 表示不限制）
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// 尝试获取入站连接准入许可；满载时等待短宽限，仍超限则记录告警并返回 None
    pub async fn acquire(&self) -> Option<InboundConnectionGuard> {
        let permit = match &self.semaphore {
            None => None,
            Some(sem) => match sem.clone().try_acquire_owned() {
                Ok(p) => Some(p),
                Err(_) => {
                    match tokio::time::timeout(ACQUIRE_GRACE_TIMEOUT, sem.clone().acquire_owned())
                        .await
                    {
                        Ok(Ok(p)) => Some(p),
                        Ok(Err(_)) | Err(_) => {
                            tracing::warn!(
                                active = self.active_connections(),
                                max = self.max_connections,
                                "入站连接数达到上限且宽限超时，主动拒绝新连接以保护 FD"
                            );
                            return None;
                        }
                    }
                }
            },
        };

        self.active_count.fetch_add(1, Ordering::Relaxed);
        Some(InboundConnectionGuard {
            _permit: permit,
            active_count: self.active_count.clone(),
        })
    }
}

/// 为新接收的 TCP 连接启用 TCP_NODELAY，消除 Nagle + Delayed ACK 导致的 40ms 尾部延迟
#[inline]
pub fn configure_tcp_stream(stream: &TcpStream) {
    let _ = stream.set_nodelay(true);
}

/// 使用 HTTP/1.1 + HTTP/2 (h2c) 自适应双协议驱动单个入站连接（保留 WebSocket Upgrade 能力）
pub async fn serve_inbound_connection<S, B>(
    stream: TcpStream,
    service: S,
    cancel_token: CancellationToken,
    _guard: InboundConnectionGuard,
) where
    S: Service<Request<Incoming>, Response = hyper::Response<B>>,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn Error + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn Error + Send + Sync>>,
{
    configure_tcp_stream(&stream);
    let io = TokioIo::new(stream);

    let mut builder = AutoConnBuilder::new(TokioExecutor::new());
    builder.http1().keep_alive(true);
    builder
        .http2()
        .adaptive_window(true)
        .max_concurrent_streams(256);

    let conn = builder.serve_connection_with_upgrades(io, service);
    tokio::pin!(conn);

    tokio::select! {
        res = conn.as_mut() => {
            if let Err(err) = res {
                tracing::debug!("连接处理结束或出错: {:?}", err);
            }
        }
        _ = cancel_token.cancelled() => {
            conn.as_mut().graceful_shutdown();
            let _ = tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, conn).await;
        }
    }
}
