//! ProxyTunnel — HTTP tunnel through the Wire mesh.
//!
//! Provides the target-side handler: when a node receives a
//! `_wire_tunnel_req` JSON message, this module makes the actual
//! upstream HTTP call and produces a `_wire_tunnel_res` response.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::Request;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use log::error;
use serde_json::Value;
use std::collections::HashMap;

/// Hop-by-hop headers that must not be forwarded.
const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
];

fn is_hop_by_hop(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    HOP_BY_HOP.iter().any(|h| *h == lower)
}

/// Execute a `_wire_tunnel_req`: make the upstream HTTP call and return
/// a `_wire_tunnel_res` JSON value ready to be sent back to the originator.
pub async fn execute_tunnel_request(req_data: &Value) -> Value {
    let req_id = req_data["id"].as_str().unwrap_or("");
    let method = req_data["method"].as_str().unwrap_or("GET");
    let path = req_data["path"].as_str().unwrap_or("/");
    let upstream_url = req_data["upstream_url"].as_str().unwrap_or("");
    let body_b64 = req_data["body_b64"].as_str().unwrap_or("");
    let headers_val = req_data.get("headers");

    let url = format!(
        "{}{}",
        upstream_url.trim_end_matches('/'),
        path
    );

    let body_bytes = if body_b64.is_empty() {
        Bytes::new()
    } else {
        match BASE64.decode(body_b64) {
            Ok(b) => Bytes::from(b),
            Err(_) => Bytes::new(),
        }
    };

    let uri: hyper::Uri = match url.parse() {
        Ok(u) => u,
        Err(e) => {
            error!("Invalid tunnel upstream URI {}: {}", url, e);
            return make_error_response(req_id, &format!("Invalid URI: {}", e));
        }
    };

    let parsed_method: hyper::Method = match method.parse() {
        Ok(m) => m,
        Err(e) => {
            error!("Invalid HTTP method {}: {}", method, e);
            return make_error_response(req_id, &format!("Invalid method: {}", e));
        }
    };

    let mut builder = Request::builder().method(parsed_method).uri(&uri);

    // Forward request headers
    if let Some(Value::Object(hmap)) = headers_val {
        for (k, v) in hmap {
            if !is_hop_by_hop(k) {
                if let Some(val) = v.as_str() {
                    builder = builder.header(k.as_str(), val);
                }
            }
        }
    }

    let upstream_req = match builder.body(Full::new(body_bytes)) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to build tunnel upstream request: {}", e);
            return make_error_response(req_id, &format!("Request build error: {}", e));
        }
    };

    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let upstream_resp = match client.request(upstream_req).await {
        Ok(resp) => resp,
        Err(e) => {
            error!("Tunnel upstream error for {}: {}", url, e);
            return make_error_response(req_id, &format!("Upstream error: {}", e));
        }
    };

    let status = upstream_resp.status().as_u16();

    // Collect response headers
    let mut resp_headers: HashMap<String, String> = HashMap::new();
    for (name, value) in upstream_resp.headers() {
        if !is_hop_by_hop(name.as_str()) {
            if let Ok(v) = value.to_str() {
                resp_headers.insert(name.to_string(), v.to_string());
            }
        }
    }

    // Read response body
    let resp_body = match upstream_resp.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            error!("Failed to read tunnel upstream response: {}", e);
            return make_error_response(req_id, &format!("Response read error: {}", e));
        }
    };

    serde_json::json!({
        "_wire_tunnel_res": {
            "id": req_id,
            "status": status,
            "headers": resp_headers,
            "body_b64": BASE64.encode(&resp_body),
        }
    })
}

fn make_error_response(req_id: &str, error_msg: &str) -> Value {
    serde_json::json!({
        "_wire_tunnel_res": {
            "id": req_id,
            "status": 502,
            "headers": {"Content-Type": "text/plain"},
            "body_b64": BASE64.encode(error_msg.as_bytes()),
        }
    })
}
