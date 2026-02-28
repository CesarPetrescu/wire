/// API versioning with reverse proxy — route /api/v1 and /api/v2 to different backends.
///
/// Longest-prefix matching ensures that /api/v2/* goes to the v2 backend
/// while /api/v1/* (or any other /api/* path) goes to the v1 backend.
///
/// Usage (from rust/wire-rs):
///     cargo run --example versioned_api_proxy

use wire_rs::proxy::ReverseProxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let mut proxy = ReverseProxy::new("0.0.0.0", 8080);

    // v2 has its own backend; v1 is the default for /api
    proxy.add_route("/api/v2", "http://localhost:3002").await;
    proxy.add_route("/api", "http://localhost:3001").await;

    proxy.start().await?;
    println!("Versioned API proxy running on http://0.0.0.0:8080");
    println!("  /api/v2/* -> http://localhost:3002  (new backend)");
    println!("  /api/*    -> http://localhost:3001  (legacy backend)");

    tokio::signal::ctrl_c().await?;
    proxy.stop().await;
    Ok(())
}
