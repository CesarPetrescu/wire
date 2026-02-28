//! ReverseProxy — lightweight async HTTP reverse proxy.
//!
//! Maps URL path prefixes to upstream HTTP services, forwarding requests
//! and streaming responses back. Useful for exposing multiple Docker
//! containers (or any HTTP backends) through a single entry point.
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//! use wire_rs::proxy::ReverseProxy;
//! let mut proxy = ReverseProxy::new("0.0.0.0", 8080);
//! proxy.add_route("/api", "http://backend:3000");
//! proxy.add_route("/dashboard", "http://frontend:8080");
//! proxy.start().await?;
//! // ... later
//! proxy.stop().await;
//! # Ok(())
//! # }
//! ```

use http_body_util::{BodyExt, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1 as server_http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use log::{error, info, warn};
use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{watch, RwLock};

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
];

fn is_hop_by_hop(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    HOP_BY_HOP.iter().any(|h| *h == lower)
}

/// Shared route table: path-prefix → upstream base URL.
type RouteTable = Arc<RwLock<HashMap<String, String>>>;

pub struct ReverseProxy {
    host: String,
    port: u16,
    routes: RouteTable,
    shutdown_tx: Option<watch::Sender<bool>>,
}

impl ReverseProxy {
    pub fn new(host: &str, port: u16) -> Self {
        ReverseProxy {
            host: host.to_string(),
            port,
            routes: Arc::new(RwLock::new(HashMap::new())),
            shutdown_tx: None,
        }
    }

    /// Register a path prefix → upstream URL mapping.
    ///
    /// The prefix is normalised to start with `/` and not end with `/`.
    /// The upstream URL's trailing slash is stripped as well.
    pub async fn add_route(&self, path_prefix: &str, upstream_url: &str) {
        let prefix = normalise_prefix(path_prefix);
        let upstream = upstream_url.trim_end_matches('/').to_string();
        info!("Route added: {} -> {}", prefix, upstream);
        self.routes.write().await.insert(prefix, upstream);
    }

    /// Remove a previously registered route.
    pub async fn remove_route(&self, path_prefix: &str) {
        let prefix = normalise_prefix(path_prefix);
        self.routes.write().await.remove(&prefix);
        info!("Route removed: {}", prefix);
    }

    /// Return a snapshot of the current route table.
    pub async fn routes_snapshot(&self) -> HashMap<String, String> {
        self.routes.read().await.clone()
    }

    /// Start the HTTP proxy server.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let addr: SocketAddr = format!("{}:{}", self.host, self.port).parse()?;
        let listener = TcpListener::bind(addr).await?;
        info!("ReverseProxy listening on http://{}", addr);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let routes = self.routes.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, _remote)) => {
                                let routes = routes.clone();
                                let io = hyper_util::rt::TokioIo::new(stream);
                                tokio::spawn(async move {
                                    let svc = service_fn(move |req| {
                                        let routes = routes.clone();
                                        async move { handle_request(req, routes).await }
                                    });
                                    if let Err(e) = server_http1::Builder::new()
                                        .serve_connection(io, svc)
                                        .await
                                    {
                                        error!("Connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => error!("Accept error: {}", e),
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("ReverseProxy shutting down.");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    /// Shut down the proxy server.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
        info!("ReverseProxy stopped.");
    }
}

fn normalise_prefix(raw: &str) -> String {
    let trimmed = raw.trim_matches('/');
    if trimmed.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", trimmed)
    }
}

/// Find the longest matching prefix for `path` in the route table.
fn match_route(routes: &HashMap<String, String>, path: &str) -> Option<(String, String)> {
    let mut best: Option<(&str, &str)> = None;
    for (prefix, upstream) in routes {
        let matches = if prefix == "/" {
            true
        } else {
            path == prefix.as_str() || path.starts_with(&format!("{}/", prefix))
        };
        if matches {
            if best.is_none() || prefix.len() > best.unwrap().0.len() {
                best = Some((prefix.as_str(), upstream.as_str()));
            }
        }
    }

    best.map(|(prefix, upstream)| {
        let remainder = if prefix == "/" {
            path.to_string()
        } else {
            let r = &path[prefix.len()..];
            if r.is_empty() || !r.starts_with('/') {
                format!("/{}", r.trim_start_matches('/'))
            } else {
                r.to_string()
            }
        };
        (upstream.to_string(), remainder)
    })
}

async fn handle_request(
    req: Request<Incoming>,
    routes: RouteTable,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(|q| q.to_string());

    let routes_read = routes.read().await;
    let matched = match_route(&routes_read, &path);
    drop(routes_read);

    let (upstream, remainder) = match matched {
        Some(m) => m,
        None => {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("No matching route")))
                .unwrap());
        }
    };

    let target_url = if let Some(q) = &query {
        format!("{}{}?{}", upstream, remainder, q)
    } else {
        format!("{}{}", upstream, remainder)
    };

    let target_uri: hyper::Uri = match target_url.parse() {
        Ok(u) => u,
        Err(e) => {
            error!("Invalid upstream URI {}: {}", target_url, e);
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from("Bad Gateway")))
                .unwrap());
        }
    };

    // Build forwarded request
    let method = req.method().clone();
    let mut builder = Request::builder().method(method).uri(&target_uri);

    // Forward headers, strip hop-by-hop and host
    for (name, value) in req.headers() {
        let key = name.as_str();
        if !is_hop_by_hop(key) && key != "host" {
            builder = builder.header(name.clone(), value.clone());
        }
    }

    // Add X-Forwarded-* headers
    builder = builder.header("X-Forwarded-Proto", "http");
    if let Some(host) = req.headers().get("host") {
        builder = builder.header("X-Forwarded-Host", host.clone());
    }
    // For X-Forwarded-For we don't have the remote IP here easily;
    // set a placeholder that could be enhanced later.
    builder = builder.header("X-Forwarded-For", "unknown");

    // Read the incoming body
    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            error!("Failed to read request body: {}", e);
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from("Bad Gateway")))
                .unwrap());
        }
    };

    let upstream_req = match builder.body(Full::new(body_bytes)) {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to build upstream request: {}", e);
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from("Bad Gateway")))
                .unwrap());
        }
    };

    // Send to upstream
    let client = Client::builder(TokioExecutor::new()).build_http::<Full<Bytes>>();
    let upstream_resp = match client.request(upstream_req).await {
        Ok(resp) => resp,
        Err(e) => {
            warn!("Upstream error for {}: {}", target_url, e);
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from("Bad Gateway")))
                .unwrap());
        }
    };

    // Build response back to the client
    let status = upstream_resp.status();
    let mut resp_builder = Response::builder().status(status);

    for (name, value) in upstream_resp.headers() {
        if !is_hop_by_hop(name.as_str()) {
            resp_builder = resp_builder.header(name.clone(), value.clone());
        }
    }

    let resp_body = match upstream_resp.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            error!("Failed to read upstream response: {}", e);
            return Ok(Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .body(Full::new(Bytes::from("Bad Gateway")))
                .unwrap());
        }
    };

    Ok(resp_builder
        .body(Full::new(resp_body))
        .unwrap_or_else(|_| {
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Full::new(Bytes::from("Internal Server Error")))
                .unwrap()
        }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalise_prefix() {
        assert_eq!(normalise_prefix("/api"), "/api");
        assert_eq!(normalise_prefix("/api/"), "/api");
        assert_eq!(normalise_prefix("api"), "/api");
        assert_eq!(normalise_prefix("/"), "/");
        assert_eq!(normalise_prefix(""), "/");
    }

    #[test]
    fn test_match_route_exact() {
        let mut routes = HashMap::new();
        routes.insert("/api".to_string(), "http://backend:3000".to_string());
        let (upstream, remainder) = match_route(&routes, "/api").unwrap();
        assert_eq!(upstream, "http://backend:3000");
        assert_eq!(remainder, "/");
    }

    #[test]
    fn test_match_route_subpath() {
        let mut routes = HashMap::new();
        routes.insert("/api".to_string(), "http://backend:3000".to_string());
        let (upstream, remainder) = match_route(&routes, "/api/users/42").unwrap();
        assert_eq!(upstream, "http://backend:3000");
        assert_eq!(remainder, "/users/42");
    }

    #[test]
    fn test_match_route_no_match() {
        let mut routes = HashMap::new();
        routes.insert("/api".to_string(), "http://backend:3000".to_string());
        assert!(match_route(&routes, "/dashboard").is_none());
    }

    #[test]
    fn test_match_route_longest_prefix() {
        let mut routes = HashMap::new();
        routes.insert("/api".to_string(), "http://general:3000".to_string());
        routes.insert("/api/v2".to_string(), "http://v2:3001".to_string());
        let (upstream, remainder) = match_route(&routes, "/api/v2/items").unwrap();
        assert_eq!(upstream, "http://v2:3001");
        assert_eq!(remainder, "/items");
    }

    #[test]
    fn test_match_route_root_catches_all() {
        let mut routes = HashMap::new();
        routes.insert("/".to_string(), "http://default:80".to_string());
        let (upstream, remainder) = match_route(&routes, "/anything/here").unwrap();
        assert_eq!(upstream, "http://default:80");
        assert_eq!(remainder, "/anything/here");
    }
}
