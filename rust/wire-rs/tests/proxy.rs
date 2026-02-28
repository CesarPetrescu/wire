//! Integration tests for the ReverseProxy.

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::server::conn::http1 as server_http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use std::sync::atomic::{AtomicU16, Ordering};
use tokio::net::TcpListener;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(22000);

fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Spin up a tiny echo HTTP server that returns request info as JSON.
async fn start_echo_server(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    let listener = TcpListener::bind(&addr).await.unwrap();

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(v) => v,
                Err(_) => break,
            };
            let io = hyper_util::rt::TokioIo::new(stream);
            tokio::spawn(async move {
                let svc = service_fn(|req: Request<hyper::body::Incoming>| async move {
                    let method = req.method().to_string();
                    let path = req.uri().path().to_string();
                    let query = req.uri().query().unwrap_or("").to_string();

                    // Collect headers
                    let mut headers_map = serde_json::Map::new();
                    for (name, value) in req.headers() {
                        headers_map.insert(
                            name.as_str().to_string(),
                            serde_json::Value::String(
                                value.to_str().unwrap_or("").to_string(),
                            ),
                        );
                    }

                    let body_bytes = req
                        .into_body()
                        .collect()
                        .await
                        .unwrap()
                        .to_bytes();
                    let body_str = String::from_utf8_lossy(&body_bytes).to_string();

                    let json = serde_json::json!({
                        "method": method,
                        "path": path,
                        "query": query,
                        "headers": headers_map,
                        "body": body_str,
                    });

                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(200)
                            .header("Content-Type", "application/json")
                            .body(Full::new(Bytes::from(serde_json::to_vec(&json).unwrap())))
                            .unwrap(),
                    )
                });
                let _ = server_http1::Builder::new()
                    .serve_connection(io, svc)
                    .await;
            });
        }
    });

    // Give the server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
}

#[tokio::test]
async fn test_get_forwarded() {
    let echo_port = next_port();
    let proxy_port = next_port();
    start_echo_server(echo_port).await;

    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/api", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .uri(format!("http://127.0.0.1:{}/api/hello", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["method"], "GET");
    assert_eq!(json["path"], "/hello");
}

#[tokio::test]
async fn test_post_with_body() {
    let echo_port = next_port();
    let proxy_port = next_port();
    start_echo_server(echo_port).await;

    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/api", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .method("POST")
                .uri(format!("http://127.0.0.1:{}/api/data", proxy_port))
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(r#"{"key":"value"}"#)))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["method"], "POST");
    assert_eq!(json["body"], r#"{"key":"value"}"#);
}

#[tokio::test]
async fn test_query_string_forwarded() {
    let echo_port = next_port();
    let proxy_port = next_port();
    start_echo_server(echo_port).await;

    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/api", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .uri(format!(
                    "http://127.0.0.1:{}/api/search?q=hello&page=2",
                    proxy_port
                ))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["query"], "q=hello&page=2");
}

#[tokio::test]
async fn test_x_forwarded_headers() {
    let echo_port = next_port();
    let proxy_port = next_port();
    start_echo_server(echo_port).await;

    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/api", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .uri(format!("http://127.0.0.1:{}/api/check", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let headers = json["headers"].as_object().unwrap();
    assert!(headers.contains_key("x-forwarded-proto"));
    assert!(headers.contains_key("x-forwarded-for"));
}

#[tokio::test]
async fn test_404_no_matching_route() {
    let proxy_port = next_port();
    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/api", "http://127.0.0.1:1")
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .uri(format!("http://127.0.0.1:{}/unknown/path", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_502_unreachable_upstream() {
    let proxy_port = next_port();
    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    // Port 1 — nothing listens there
    proxy
        .add_route("/dead", "http://127.0.0.1:1")
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .uri(format!("http://127.0.0.1:{}/dead/test", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn test_multiple_routes() {
    let echo_port = next_port();
    let proxy_port = next_port();
    start_echo_server(echo_port).await;

    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/svc-a", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy
        .add_route("/svc-b", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    let resp = client
        .request(
            Request::builder()
                .uri(format!("http://127.0.0.1:{}/svc-a/foo", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["path"], "/foo");

    let resp = client
        .request(
            Request::builder()
                .uri(format!("http://127.0.0.1:{}/svc-b/bar", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["path"], "/bar");
}

#[tokio::test]
async fn test_put_and_delete_methods() {
    let echo_port = next_port();
    let proxy_port = next_port();
    start_echo_server(echo_port).await;

    let mut proxy = wire_rs::proxy::ReverseProxy::new("127.0.0.1", proxy_port);
    proxy
        .add_route("/api", &format!("http://127.0.0.1:{}", echo_port))
        .await;
    proxy.start().await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    let client =
        hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
            .build_http::<Full<Bytes>>();

    // PUT
    let resp = client
        .request(
            Request::builder()
                .method("PUT")
                .uri(format!("http://127.0.0.1:{}/api/item/1", proxy_port))
                .body(Full::new(Bytes::from("updated")))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["method"], "PUT");
    assert_eq!(json["body"], "updated");

    // DELETE
    let resp = client
        .request(
            Request::builder()
                .method("DELETE")
                .uri(format!("http://127.0.0.1:{}/api/item/1", proxy_port))
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["method"], "DELETE");
}
