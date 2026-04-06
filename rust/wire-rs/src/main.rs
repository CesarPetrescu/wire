//! Wire — WebSocket SSL bidirectional communication framework.
//!
//! Usage:
//!   wire start -c wire.yaml       Start daemon from config
//!   wire validate -c wire.yaml    Validate config file
//!   wire gen-secret               Generate a random pre-shared secret
//!
//! Legacy (still supported):
//!   wire controller [--host H] [--port P] [--secret S]
//!   wire sub [--host H] [--port P] [--secret S]

use serde_json::json;
use std::env;
use url::Url;
use wire_rs::config;
use wire_rs::controller::Controller;
use wire_rs::proxy::ReverseProxy;
use wire_rs::subcontroller::{ServiceDef, SubController};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: wire <start|validate|gen-secret|controller|sub> [options]\n\
             \n\
             Commands:\n\
             \x20 start -c <config.yaml>     Start daemon from config\n\
             \x20 validate -c <config.yaml>  Validate config file\n\
             \x20 gen-secret                 Generate a random pre-shared secret\n\
             \x20 controller [flags]         Legacy: start as controller\n\
             \x20 sub [flags]               Legacy: start as subcontroller"
        );
        std::process::exit(1);
    }

    match args[1].as_str() {
        "start" => cmd_start(&args).await?,
        "validate" => cmd_validate(&args)?,
        "gen-secret" => cmd_gen_secret(),
        // Legacy subcommands
        "controller" => cmd_legacy_controller(&args).await?,
        "sub" => cmd_legacy_sub(&args).await?,
        other => {
            eprintln!("Unknown command: {}. Use 'start', 'validate', 'gen-secret', 'controller', or 'sub'.", other);
            std::process::exit(1);
        }
    }

    Ok(())
}

fn find_config_path(args: &[String]) -> Option<String> {
    for i in 0..args.len() {
        if (args[i] == "-c" || args[i] == "--config") && i + 1 < args.len() {
            return Some(args[i + 1].clone());
        }
    }
    None
}

async fn cmd_start(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = find_config_path(args).unwrap_or_else(|| {
        eprintln!("Error: -c <config.yaml> required for 'start'");
        std::process::exit(1);
    });

    let cfg = config::load_config(&config_path)?;
    let errors = config::validate_config(&cfg);
    if !errors.is_empty() {
        for e in &errors {
            eprintln!("Config error: {}", e);
        }
        std::process::exit(1);
    }

    let secret = cfg.auth.resolved_secret();

    match cfg.role.as_str() {
        "controller" => {
            let mut ctrl = Controller::new(&cfg.listen.host, cfg.listen.port, &secret);
            let mut rx = ctrl.message_rx.take().unwrap();
            ctrl.start().await?;
            println!(
                "Controller running on wss://{}:{}. Press Ctrl+C to stop.",
                cfg.listen.host, cfg.listen.port
            );

            // Start reverse proxy if configured
            if cfg.proxy.enabled {
                let mut proxy = ReverseProxy::new(&cfg.proxy.host, cfg.proxy.port);
                for route in &cfg.proxy.static_routes {
                    proxy.add_route(&route.prefix, &route.upstream).await;
                }
                proxy.start().await?;
                println!(
                    "ReverseProxy running on http://{}:{}.",
                    cfg.proxy.host, cfg.proxy.port
                );
            }

            // Wait for Ctrl+C
            tokio::select! {
                _ = async {
                    while let Some(msg) = rx.recv().await {
                        print_controller_msg(msg);
                    }
                } => {}
                _ = tokio::signal::ctrl_c() => {
                    println!("\nShutting down...");
                    ctrl.stop().await;
                }
            }
        }
        "sub" => {
            // Parse controller URL using the url crate
            let parsed_url = Url::parse(&cfg.controller.url).unwrap_or_else(|_| {
                // Fallback: try adding a scheme so the url crate can parse it
                Url::parse(&format!("wss://{}", cfg.controller.url))
                    .unwrap_or_else(|_| Url::parse("wss://127.0.0.1:8765").unwrap())
            });
            let ctrl_host = parsed_url.host_str().unwrap_or("127.0.0.1").to_string();
            let ctrl_port = parsed_url.port().unwrap_or(8765);
            let mut sub = SubController::new(&ctrl_host, ctrl_port, &secret);

            // Set services
            if !cfg.services.is_empty() {
                let services: Vec<ServiceDef> = cfg
                    .services
                    .iter()
                    .map(|s| ServiceDef {
                        prefix: s.prefix.clone(),
                        upstream: s.upstream.clone(),
                        health_check: if s.health_check.is_empty() {
                            None
                        } else {
                            Some(s.health_check.clone())
                        },
                    })
                    .collect();
                sub.set_services(services);
            }

            let mut rx = sub.message_rx.take().unwrap();
            sub.connect().await?;
            println!("SubController connected.");

            tokio::select! {
                _ = async {
                    while let Some(msg) = rx.recv().await {
                        print_sub_msg(msg);
                    }
                } => {}
                _ = tokio::signal::ctrl_c() => {
                    println!("\nShutting down...");
                    sub.disconnect().await;
                }
            }
        }
        _ => unreachable!(),
    }

    Ok(())
}

fn cmd_validate(args: &[String]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config_path = find_config_path(args).unwrap_or_else(|| {
        eprintln!("Error: -c <config.yaml> required for 'validate'");
        std::process::exit(1);
    });

    let cfg = config::load_config(&config_path)?;
    let errors = config::validate_config(&cfg);
    if errors.is_empty() {
        println!("Config is valid.");
        println!("  Role: {}", cfg.role);
        println!("  Listen: {}:{}", cfg.listen.host, cfg.listen.port);
        if cfg.proxy.enabled {
            println!("  Proxy: {}:{}", cfg.proxy.host, cfg.proxy.port);
        }
        if !cfg.services.is_empty() {
            println!("  Services: {}", cfg.services.len());
        }
    } else {
        for e in &errors {
            eprintln!("Error: {}", e);
        }
        std::process::exit(1);
    }
    Ok(())
}

fn cmd_gen_secret() {
    use ring::rand::{SecureRandom, SystemRandom};
    let rng = SystemRandom::new();
    let mut buf = [0u8; 32];
    rng.fill(&mut buf).expect("Failed to generate random bytes");
    println!("{}", hex::encode(buf));
}

// ── Legacy commands ──────────────────────────────────────────────────────────

fn parse_legacy_args(args: &[String]) -> (String, u16, String) {
    let mut host = "127.0.0.1".to_string();
    let mut port: u16 = 8765;
    let mut secret = "changeme".to_string();

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                host = args.get(i + 1).cloned().unwrap_or(host);
                i += 2;
            }
            "--port" => {
                port = args
                    .get(i + 1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(port);
                i += 2;
            }
            "--secret" => {
                secret = args.get(i + 1).cloned().unwrap_or(secret);
                i += 2;
            }
            _ => i += 1,
        }
    }
    (host, port, secret)
}

async fn cmd_legacy_controller(
    args: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (host, port, secret) = parse_legacy_args(args);
    let mut ctrl = Controller::new(&host, port, &secret);
    let mut rx = ctrl.message_rx.take().unwrap();
    ctrl.start().await?;
    println!(
        "Controller running on wss://{}:{}. Press Ctrl+C to stop.",
        host, port
    );

    while let Some(msg) = rx.recv().await {
        print_controller_msg(msg);
    }
    Ok(())
}

async fn cmd_legacy_sub(
    args: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (host, port, secret) = parse_legacy_args(args);
    let mut sub = SubController::new(&host, port, &secret);
    let mut rx = sub.message_rx.take().unwrap();
    sub.connect().await?;
    println!("SubController connected. Sending test JSON...");

    sub.send_json(&json!({"hello": "from rust subcontroller"}))
        .await?;

    while let Some(msg) = rx.recv().await {
        print_sub_msg(msg);
    }
    Ok(())
}

// ── Message printing ─────────────────────────────────────────────────────────

fn print_controller_msg(msg: wire_rs::controller::WireMessage) {
    match msg {
        wire_rs::controller::WireMessage::Json { peer_fp, data } => {
            println!("[JSON from {}...]: {}", &peer_fp[..16], data);
        }
        wire_rs::controller::WireMessage::Binary { peer_fp, data } => {
            println!("[BINARY from {}...]: {} bytes", &peer_fp[..16], data.len());
        }
        wire_rs::controller::WireMessage::File {
            peer_fp,
            filename,
            data,
        } => {
            println!(
                "[FILE from {}...]: {} ({} bytes)",
                &peer_fp[..16],
                filename,
                data.len()
            );
        }
        wire_rs::controller::WireMessage::Image {
            peer_fp,
            filename,
            data,
        } => {
            println!(
                "[IMAGE from {}...]: {} ({} bytes)",
                &peer_fp[..16],
                filename,
                data.len()
            );
        }
    }
}

fn print_sub_msg(msg: wire_rs::subcontroller::WireMessage) {
    match msg {
        wire_rs::subcontroller::WireMessage::Json { data } => {
            println!("[JSON from controller]: {}", data);
        }
        wire_rs::subcontroller::WireMessage::Binary { data } => {
            println!("[BINARY from controller]: {} bytes", data.len());
        }
        wire_rs::subcontroller::WireMessage::File { filename, data } => {
            println!("[FILE from controller]: {} ({} bytes)", filename, data.len());
        }
        wire_rs::subcontroller::WireMessage::Image { filename, data } => {
            println!(
                "[IMAGE from controller]: {} ({} bytes)",
                filename,
                data.len()
            );
        }
        wire_rs::subcontroller::WireMessage::RelayJson { source_fp, data } => {
            println!(
                "[RELAY JSON from {}...]: {}",
                &source_fp[..16.min(source_fp.len())],
                data
            );
        }
        wire_rs::subcontroller::WireMessage::RelayBinary { source_fp, data } => {
            println!(
                "[RELAY BINARY from {}...]: {} bytes",
                &source_fp[..16.min(source_fp.len())],
                data.len()
            );
        }
        wire_rs::subcontroller::WireMessage::RelayFile {
            source_fp,
            filename,
            data,
        } => {
            println!(
                "[RELAY FILE from {}...]: {} ({} bytes)",
                &source_fp[..16.min(source_fp.len())],
                filename,
                data.len()
            );
        }
        wire_rs::subcontroller::WireMessage::RelayImage {
            source_fp,
            filename,
            data,
        } => {
            println!(
                "[RELAY IMAGE from {}...]: {} ({} bytes)",
                &source_fp[..16.min(source_fp.len())],
                filename,
                data.len()
            );
        }
        wire_rs::subcontroller::WireMessage::PeerJoined { peer_fp } => {
            println!("[PEER JOINED]: {}", peer_fp);
        }
        wire_rs::subcontroller::WireMessage::PeerLeft { peer_fp } => {
            println!("[PEER LEFT]: {}", peer_fp);
        }
    }
}
