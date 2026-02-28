/// Dynamic route management — add and remove routes at runtime.
///
/// Demonstrates how to modify the proxy's route table while it is running.
/// This is useful when backends come and go (e.g. containers scaling up/down).
///
/// Usage (from rust/wire-rs):
///     cargo run --example dynamic_routes_proxy

use wire_rs::proxy::ReverseProxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let mut proxy = ReverseProxy::new("0.0.0.0", 8080);

    // Start with a single backend
    proxy.add_route("/api", "http://localhost:3000").await;
    proxy.start().await?;
    println!("Proxy started with /api -> http://localhost:3000");

    // Simulate a new service coming online after 5 seconds
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    proxy.add_route("/metrics", "http://localhost:9090").await;
    println!("Added /metrics -> http://localhost:9090");

    // Inspect current routes
    let routes = proxy.routes_snapshot().await;
    println!("Current routes: {:?}", routes);

    // Simulate removing a route after another 10 seconds
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    proxy.remove_route("/api").await;
    println!("Removed /api route");

    let routes = proxy.routes_snapshot().await;
    println!("Current routes: {:?}", routes);

    tokio::signal::ctrl_c().await?;
    proxy.stop().await;
    Ok(())
}
