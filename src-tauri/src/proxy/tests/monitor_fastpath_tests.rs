use crate::proxy::middleware::monitor_usage_tap::{
    run_disabled_monitor_fastpath_with_monitor, SseUsageScanner,
};
use crate::proxy::monitor::{ProxyMonitor, UpstreamRequestBodyHolder};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::Response,
    routing::post,
    Router,
};
use bytes::Bytes;
use futures::stream;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tower::ServiceExt;

#[test]
fn test_sse_usage_scanner_anthropic_split_chunks() {
    let mut scanner = SseUsageScanner::default();
    let stream_text = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":1200,\"cache_read_input_tokens\":8800,\"output_tokens\":1}}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello world\"}}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":150}}\n\n",
        "data: [DONE]\n"
    );

    // Feed in small odd-sized slices to exercise cross-chunk line buffering
    for chunk in stream_text.as_bytes().chunks(19) {
        scanner.feed(chunk);
    }
    scanner.finish();

    assert_eq!(scanner.input_tokens, Some(10000));
    assert_eq!(scanner.cached_tokens, Some(8800));
    assert_eq!(scanner.output_tokens, Some(150));
}

#[test]
fn test_sse_usage_scanner_gemini_and_responses_formats() {
    let mut gemini = SseUsageScanner::default();
    gemini.feed(
        br#"data: {"candidates":[{"content":{"parts":[{"text":"hi"}]}}],"usageMetadata":{"promptTokenCount":500,"candidatesTokenCount":120,"cachedContentTokenCount":300,"thoughtsTokenCount":45}}
"#,
    );
    gemini.finish();
    assert_eq!(gemini.input_tokens, Some(500));
    assert_eq!(gemini.output_tokens, Some(120));
    assert_eq!(gemini.cached_tokens, Some(300));
    assert_eq!(gemini.reasoning_tokens, Some(45));

    let mut responses = SseUsageScanner::default();
    responses.feed(
        br#"data: {"type":"response.completed","response":{"usage":{"input_tokens":800,"output_tokens":90,"input_tokens_details":{"cached_tokens":200},"output_tokens_details":{"reasoning_tokens":30}}}}
"#,
    );
    responses.finish();
    assert_eq!(responses.input_tokens, Some(800));
    assert_eq!(responses.output_tokens, Some(90));
    assert_eq!(responses.cached_tokens, Some(200));
    assert_eq!(responses.reasoning_tokens, Some(30));
}

#[test]
fn test_sse_usage_scanner_skips_oversized_line_without_retaining_memory() {
    let mut scanner = SseUsageScanner::default();
    let big_b64 = "A".repeat(70 * 1024);
    let oversized_prefix = format!("data: {{\"image\":\"{}", big_b64);

    scanner.feed(oversized_prefix.as_bytes());
    // Oversized incomplete line must be dropped immediately instead of retained
    assert_eq!(scanner.buffered_len(), 0);

    scanner.feed(b"\"}\n");
    scanner.feed(b"data: {\"usage\":{\"prompt_tokens\":42,\"completion_tokens\":7}}\n");
    scanner.finish();

    assert_eq!(scanner.buffered_len(), 0);
    assert_eq!(scanner.input_tokens, Some(42));
    assert_eq!(scanner.output_tokens, Some(7));
}

#[tokio::test]
async fn test_fastpath_does_not_buffer_request_body_or_inject_upstream_holder() {
    let monitor = Arc::new(ProxyMonitor::new_for_test(16));
    assert!(!monitor.is_enabled());

    let body_polled_in_handler = Arc::new(AtomicBool::new(false));
    let body_polled_flag = body_polled_in_handler.clone();

    async fn fastpath_mw(
        State(monitor): State<Arc<ProxyMonitor>>,
        request: Request,
        next: Next,
    ) -> Response {
        run_disabled_monitor_fastpath_with_monitor(monitor, request, next).await
    }

    let app = Router::new()
        .route(
            "/v1/messages",
            post(move |req: Request| {
                let flag = body_polled_flag.clone();
                async move {
                    assert!(
                        req.extensions().get::<UpstreamRequestBodyHolder>().is_none(),
                        "UpstreamRequestBodyHolder must not be injected when monitor is disabled"
                    );
                    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
                        .await
                        .expect("handler should receive unconsumed request body");
                    assert_eq!(&bytes[..], b"{\"model\":\"claude-sonnet-4-5\",\"stream\":true}");
                    flag.store(true, Ordering::SeqCst);

                    let sse = concat!(
                        "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n",
                        "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":25}}\n\n"
                    );
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(header::CONTENT_TYPE, "text/event-stream")
                        .header("X-Account-Email", "fastpath@example.com")
                        .body(Body::from(sse))
                        .unwrap()
                }
            }),
        )
        .layer(from_fn_with_state(monitor, fastpath_mw));

    let req_stream = stream::iter(vec![
        Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{\"model\":\"claude-sonnet-4-5\",")),
        Ok::<Bytes, std::io::Error>(Bytes::from_static(b"\"stream\":true}")),
    ]);
    let request = Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .body(Body::from_stream(req_stream))
        .unwrap();

    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(body_polled_in_handler.load(Ordering::SeqCst));

    let resp_bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let resp_text = std::str::from_utf8(&resp_bytes).unwrap();
    assert!(resp_text.contains("\"output_tokens\":25"));
}
