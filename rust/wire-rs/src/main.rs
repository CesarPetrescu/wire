//! Wire — WebSocket SSL bidirectional communication framework.
//!
//! Run as controller:  cargo run -- controller --port 8765 --secret mysecret
//! Run as subcontroller: cargo run -- sub --host 127.0.0.1 --port 8765 --secret mysecret

use serde_json::json;
use std::env;
use wire_rs::controller::Controller;
use wire_rs::subcontroller::SubController;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: wire_rs <controller|sub> [--host H] [--port P] [--secret S]");
        std::process::exit(1);
    }

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

    match args[1].as_str() {
        "controller" => {
            let mut ctrl = Controller::new(&host, port, &secret);
            let mut rx = ctrl.message_rx.take().unwrap();
            ctrl.start().await?;
            println!("Controller running on wss://{}:{}. Press Ctrl+C to stop.", host, port);

            while let Some(msg) = rx.recv().await {
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
        }
        "sub" => {
            let mut sub = SubController::new(&host, port, &secret);
            let mut rx = sub.message_rx.take().unwrap();
            sub.connect().await?;
            println!("SubController connected. Sending test JSON...");

            sub.send_json(&json!({"hello": "from rust subcontroller"}))
                .await?;

            while let Some(msg) = rx.recv().await {
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
                        println!("[RELAY JSON from {}...]: {}", &source_fp[..16.min(source_fp.len())], data);
                    }
                    wire_rs::subcontroller::WireMessage::RelayBinary { source_fp, data } => {
                        println!("[RELAY BINARY from {}...]: {} bytes", &source_fp[..16.min(source_fp.len())], data.len());
                    }
                    wire_rs::subcontroller::WireMessage::RelayFile { source_fp, filename, data } => {
                        println!("[RELAY FILE from {}...]: {} ({} bytes)", &source_fp[..16.min(source_fp.len())], filename, data.len());
                    }
                    wire_rs::subcontroller::WireMessage::RelayImage { source_fp, filename, data } => {
                        println!("[RELAY IMAGE from {}...]: {} ({} bytes)", &source_fp[..16.min(source_fp.len())], filename, data.len());
                    }
                    wire_rs::subcontroller::WireMessage::PeerJoined { peer_fp } => {
                        println!("[PEER JOINED]: {}", peer_fp);
                    }
                    wire_rs::subcontroller::WireMessage::PeerLeft { peer_fp } => {
                        println!("[PEER LEFT]: {}", peer_fp);
                    }
                }
            }
        }
        _ => {
            eprintln!("Unknown role: {}. Use 'controller' or 'sub'.", args[1]);
            std::process::exit(1);
        }
    }

    Ok(())
}
