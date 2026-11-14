use crate::proxy::inbound_conn::{
    configure_tcp_stream, serve_inbound_connection, InboundConnectionLimiter,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::get,
    Router,
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::service::TowerToHyperService;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

async fn spawn_test_server(limiter: InboundConnectionLimiter) -> (SocketAddr, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let app = Router::new().route("/ping", get(|| async { StatusCode::OK }));

    let server_cancel = cancel.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = server_cancel.cancelled() => break,
                res = listener.accept() => {
                    let Ok((stream, _)) = res else { break };
                    let limiter = limiter.clone();
                    let app = app.clone();
                    let conn_cancel = server_cancel.clone();
                    tokio::spawn(async move {
                        let Some(guard) = limiter.acquire().await else {
                            drop(stream);
                            return;
                        };
                        let service = TowerToHyperService::new(app);
                        serve_inbound_connection(stream, service, conn_cancel, guard).await;
                    });
                }
            }
        }
    });

    (addr, cancel)
}

#[tokio::test]
async fn test_tcp_nodelay_enabled() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
    let (server_stream, _) = listener.accept().await.unwrap();
    configure_tcp_stream(&server_stream);
    assert!(server_stream.nodelay().unwrap());
    let _ = client.await;
}

async fn http1_connect(
    addr: SocketAddr,
) -> (
    hyper::client::conn::http1::SendRequest<Body>,
    tokio::task::JoinHandle<()>,
) {
    let stream = TcpStream::connect(addr).await.unwrap();
    let (sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .unwrap();
    let handle = tokio::spawn(async move {
        let _ = conn.await;
    });
    (sender, handle)
}

fn ping_req() -> Request<Body> {
    Request::builder().uri("/ping").body(Body::empty()).unwrap()
}

#[tokio::test]
async fn test_http1_request_returns_200() {
    let (addr, cancel) = spawn_test_server(InboundConnectionLimiter::new(16)).await;
    let (mut sender, _handle) = http1_connect(addr).await;
    let resp = sender.send_request(ping_req()).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    cancel.cancel();
}

#[tokio::test]
async fn test_http2_h2c_multiplexed_requests() {
    let (addr, cancel) = spawn_test_server(InboundConnectionLimiter::new(16)).await;
    let stream = TcpStream::connect(addr).await.unwrap();
    let (sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut tasks = Vec::with_capacity(10);
    for _ in 0..10 {
        let mut s = sender.clone();
        tasks.push(tokio::spawn(async move {
            s.ready().await.unwrap();
            s.send_request(ping_req()).await.unwrap().status()
        }));
    }

    for task in tasks {
        assert_eq!(task.await.unwrap(), StatusCode::OK);
    }
    cancel.cancel();
}

#[tokio::test]
async fn test_connection_limiter_rejects_and_recovers() {
    let limiter = InboundConnectionLimiter::new(2);
    let (addr, cancel) = spawn_test_server(limiter.clone()).await;

    let mut held = Vec::new();
    for _ in 0..2 {
        let (mut sender, handle) = http1_connect(addr).await;
        assert_eq!(
            sender.send_request(ping_req()).await.unwrap().status(),
            StatusCode::OK
        );
        held.push((sender, handle));
    }
    assert_eq!(limiter.active_connections(), 2);

    // 第 3 条超限连接在 250ms 宽限超时后应被拒绝并关闭
    let (mut sender3, _handle3) = http1_connect(addr).await;
    assert!(sender3.send_request(ping_req()).await.is_err());

    // 释放存量连接后，新连接应立即恢复正常服务
    drop(held);
    for _ in 0..50 {
        if limiter.active_connections() == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(limiter.active_connections(), 0);

    let (mut sender4, _handle4) = http1_connect(addr).await;
    assert_eq!(
        sender4.send_request(ping_req()).await.unwrap().status(),
        StatusCode::OK
    );
    cancel.cancel();
}
