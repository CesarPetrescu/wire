/// Multi-service reverse proxy — route by path prefix.
///
/// Expose multiple backend services through a single entry point.
/// Requests are dispatched based on the URL path prefix:
///
///     /api/*        -> http://localhost:3000  (REST API)
///     /auth/*       -> http://localhost:3001  (Auth service)
///     /dashboard/*  -> http://localhost:8081  (Frontend dashboard)
///     /*            -> http://localhost:8082  (Default / landing page)
///
/// Usage (from rust/wire-rs):
///     cargo run --example multi_service_proxy

use wire_rs::proxy::ReverseProxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let mut proxy = ReverseProxy::new("0.0.0.0", 8080);

    // Most specific prefixes first (order doesn't matter — longest prefix wins)
    proxy.add_route("/api", "http://localhost:3000").await;
    proxy.add_route("/auth", "http://localhost:3001").await;
    proxy.add_route("/dashboard", "http://localhost:8081").await;

    // Catch-all for anything else
    proxy.add_route("/", "http://localhost:8082").await;

    proxy.start().await?;
    println!("Multi-service proxy running on http://0.0.0.0:8080");
    println!("  /api/*       -> http://localhost:3000");
    println!("  /auth/*      -> http://localhost:3001");
    println!("  /dashboard/* -> http://localhost:8081");
    println!("  /*           -> http://localhost:8082");

    tokio::signal::ctrl_c().await?;
    proxy.stop().await;
    Ok(())
}
