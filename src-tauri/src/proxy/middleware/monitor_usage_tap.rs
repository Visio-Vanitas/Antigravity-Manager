use super::auth::UserTokenIdentity;
use super::monitor::{
    extract_cached_tokens, extract_input_tokens, extract_model_from_uri, extract_output_tokens,
    extract_protocol_from_uri, extract_reasoning_tokens, next_chunk_while_receiver_open,
    record_user_token_usage,
};
use crate::proxy::monitor::{ProxyMonitor, ProxyRequestLog};
use crate::proxy::pipeline::timing::StreamTtftCapture;
use crate::proxy::server::AppState;
use axum::{body::Body, extract::Request, http::HeaderMap, middleware::Next, response::Response};
use serde_json::Value;
use std::sync::Arc;
use std::time::Instant;

const MAX_SSE_LINE_BYTES: usize = 64 * 1024;
const MAX_NON_STREAM_READ: usize = 100 * 1024 * 1024;
const FASTPATH_JSON_PARSE_LIMIT: usize = 2 * 1024 * 1024;

#[derive(Default)]
pub(crate) struct SseUsageScanner {
    line_buf: Vec<u8>,
    skip_line: bool,
    pub input_tokens: Option<u32>,
    pub output_tokens: Option<u32>,
    pub cached_tokens: Option<u32>,
    pub reasoning_tokens: Option<u32>,
}

impl SseUsageScanner {
    pub(crate) fn feed(&mut self, mut chunk: &[u8]) {
        while let Some(nl) = chunk.iter().position(|&b| b == b'\n') {
            if !self.skip_line {
                if self.line_buf.is_empty() && nl <= MAX_SSE_LINE_BYTES {
                    self.process_line(&chunk[..nl]);
                } else if self.line_buf.len() + nl <= MAX_SSE_LINE_BYTES {
                    self.line_buf.extend_from_slice(&chunk[..nl]);
                    let line = std::mem::take(&mut self.line_buf);
                    self.process_line(&line);
                } else {
                    self.line_buf.clear();
                }
            }
            self.skip_line = false;
            chunk = &chunk[nl + 1..];
        }
        if !chunk.is_empty() && !self.skip_line {
            if self.line_buf.len() + chunk.len() > MAX_SSE_LINE_BYTES {
                self.line_buf.clear();
                self.skip_line = true;
            } else {
                self.line_buf.extend_from_slice(chunk);
            }
        }
    }

    pub(crate) fn finish(&mut self) {
        if !self.skip_line && !self.line_buf.is_empty() {
            let line = std::mem::take(&mut self.line_buf);
            self.process_line(&line);
        }
        self.line_buf.clear();
        self.skip_line = false;
    }

    #[cfg(test)]
    pub(crate) fn buffered_len(&self) -> usize {
        self.line_buf.len()
    }

    fn process_line(&mut self, raw: &[u8]) {
        let Ok(line) = std::str::from_utf8(raw) else {
            return;
        };
        let Some(data) = line.trim().strip_prefix("data:") else {
            return;
        };
        let json_str = data.trim();
        if json_str.is_empty()
            || json_str == "[DONE]"
            || (!json_str.contains("\"usage\"") && !json_str.contains("\"usageMetadata\""))
        {
            return;
        }
        if let Ok(json) = serde_json::from_str::<Value>(json_str) {
            self.apply_json_usage(&json);
        }
    }

    fn apply_json_usage(&mut self, json: &Value) {
        let Some(usage) = json
            .get("usage")
            .or_else(|| json.get("usageMetadata"))
            .or_else(|| json.get("message").and_then(|m| m.get("usage")))
            .or_else(|| json.get("delta").and_then(|d| d.get("usage")))
            .or_else(|| json.get("response").and_then(|r| r.get("usage")))
            .or_else(|| json.get("response").and_then(|r| r.get("usageMetadata")))
        else {
            return;
        };
        if let Some(v) = extract_input_tokens(usage) {
            self.input_tokens = Some(v);
        }
        if let Some(v) = extract_output_tokens(usage) {
            self.output_tokens = Some(v);
        }
        if let Some(v) = extract_cached_tokens(usage) {
            self.cached_tokens = Some(v);
        }
        if let Some(v) = extract_reasoning_tokens(usage) {
            self.reasoning_tokens = Some(v);
        }
        if self.input_tokens.is_none() && self.output_tokens.is_none() {
            self.output_tokens = usage
                .get("total_tokens")
                .or_else(|| usage.get("totalTokenCount"))
                .and_then(|v| v.as_u64())
                .map(|v| v as u32);
        }
    }

    fn apply_to_log(&self, log: &mut ProxyRequestLog) {
        log.input_tokens = self.input_tokens;
        log.output_tokens = self.output_tokens;
        log.cached_tokens = self.cached_tokens;
        log.reasoning_tokens = self.reasoning_tokens;
    }
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

fn format_ttft_headers(ttft_ms: Option<f64>) -> Option<String> {
    ttft_ms
        .filter(|&ms| ms > 0.0)
        .map(|ms| format!("{{\"x-timing-ttft-ms\":\"{:.3}\"}}", ms))
}

async fn emit_usage(
    monitor: &ProxyMonitor,
    id: &Option<UserTokenIdentity>,
    log: ProxyRequestLog,
    ua: Option<String>,
) {
    record_user_token_usage(id, &log, ua);
    monitor.log_request(log).await;
}

pub async fn run_disabled_monitor_fastpath(
    state: AppState,
    request: Request,
    next: Next,
) -> Response {
    run_disabled_monitor_fastpath_with_monitor(state.monitor, request, next).await
}

pub(crate) async fn run_disabled_monitor_fastpath_with_monitor(
    monitor: Arc<ProxyMonitor>,
    request: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = request.method().to_string();
    let uri = request.uri().to_string();
    let client_ip = request
        .headers()
        .get("x-forwarded-for")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.split(',').next().unwrap_or(s).trim().to_string())
        .or_else(|| header_str(request.headers(), "x-real-ip"));
    let user_agent = header_str(request.headers(), "user-agent");
    let user_token_identity = request.extensions().get::<UserTokenIdentity>().cloned();
    let model = extract_model_from_uri(&uri);

    let response = next.run(request).await;
    let headers = response.headers();
    let account_email = header_str(headers, "X-Account-Email");
    if account_email.is_none() && user_token_identity.is_none() {
        return response;
    }

    let content_type = header_str(headers, "content-type").unwrap_or_default();
    let initial_ttft = ["x-timing-ttft-ms", "x-timing-headers-ms"]
        .into_iter()
        .find_map(|k| {
            header_str(headers, k)?
                .parse::<f64>()
                .ok()
                .filter(|&n| n > 0.0)
        });
    let stream_ttft_capture = response.extensions().get::<StreamTtftCapture>().cloned();
    let mut log = ProxyRequestLog {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp: chrono::Utc::now().timestamp_millis(),
        method,
        url: uri.clone(),
        status: response.status().as_u16(),
        duration: start.elapsed().as_millis() as u64,
        model,
        mapped_model: header_str(headers, "X-Mapped-Model"),
        upstream_model: header_str(headers, "X-Upstream-Model"),
        account_email,
        client_ip,
        error: None,
        request_body: None,
        upstream_request_body: None,
        response_body: None,
        request_headers: None,
        upstream_request_headers: None,
        response_headers: format_ttft_headers(initial_ttft),
        input_tokens: None,
        output_tokens: None,
        cached_tokens: None,
        reasoning_tokens: None,
        protocol: extract_protocol_from_uri(&uri),
        username: user_token_identity.as_ref().map(|i| i.username.clone()),
        session_id: header_str(headers, "X-Session-Id")
            .or_else(|| header_str(headers, "X-Antigravity-Session-Id")),
    };

    if content_type.contains("text/event-stream") {
        let (parts, body) = response.into_parts();
        let mut stream = body.into_data_stream();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let mut scanner = SseUsageScanner::default();
            let mut first_chunk_ms: Option<f64> = None;
            while let Some(chunk_res) = next_chunk_while_receiver_open(&mut stream, &tx).await {
                match chunk_res {
                    Ok(chunk) => {
                        if first_chunk_ms.is_none() && !chunk.is_empty() {
                            first_chunk_ms = Some(start.elapsed().as_secs_f64() * 1000.0);
                        }
                        scanner.feed(&chunk);
                        if tx.send(Ok::<_, axum::Error>(chunk)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(axum::Error::new(e))).await;
                        break;
                    }
                }
            }
            drop(stream);
            scanner.finish();
            let ttft_ms = stream_ttft_capture
                .as_ref()
                .and_then(|c| c.get())
                .filter(|&v| v > 0.0)
                .or_else(|| first_chunk_ms.filter(|&v| v > 0.0))
                .or(initial_ttft);
            log.duration = start.elapsed().as_millis() as u64;
            log.response_headers = format_ttft_headers(ttft_ms);
            scanner.apply_to_log(&mut log);
            emit_usage(&monitor, &user_token_identity, log, user_agent).await;
        });
        Response::from_parts(
            parts,
            Body::from_stream(tokio_stream::wrappers::ReceiverStream::new(rx)),
        )
    } else if content_type.contains("application/json") {
        let (parts, body) = response.into_parts();
        match axum::body::to_bytes(body, MAX_NON_STREAM_READ).await {
            Ok(bytes) => {
                let has_marker = bytes.windows(7).any(|w| w == b"\"usage\"")
                    || bytes.windows(15).any(|w| w == b"\"usageMetadata\"");
                if bytes.len() <= FASTPATH_JSON_PARSE_LIMIT || has_marker {
                    if let Ok(json) = serde_json::from_slice::<Value>(&bytes) {
                        let mut s = SseUsageScanner::default();
                        s.apply_json_usage(&json);
                        s.apply_to_log(&mut log);
                    }
                }
                log.duration = start.elapsed().as_millis() as u64;
                emit_usage(&monitor, &user_token_identity, log, user_agent).await;
                Response::from_parts(parts, Body::from(bytes))
            }
            Err(_) => {
                emit_usage(&monitor, &user_token_identity, log, user_agent).await;
                Response::from_parts(parts, Body::empty())
            }
        }
    } else {
        emit_usage(&monitor, &user_token_identity, log, user_agent).await;
        response
    }
}
