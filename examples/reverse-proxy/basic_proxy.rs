/// Basic reverse proxy — single backend.
///
/// Exposes a backend service running on port 3000 through the proxy on port 8080.
/// All requests to http://localhost:8080/* are forwarded to http://localhost:3000/*.
///
/// Usage (from rust/wire-rs):
///     cargo run --example basic_proxy

use wire_rs::proxy::ReverseProxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let mut proxy = ReverseProxy::new("0.0.0.0", 8080);

    // Forward everything to a single backend
    proxy.add_route("/", "http://localhost:3000").await;

    proxy.start().await?;
    println!("Proxy running on http://0.0.0.0:8080 -> http://localhost:3000");

    // Run until Ctrl-C
    tokio::signal::ctrl_c().await?;
    proxy.stop().await;
    Ok(())
}
