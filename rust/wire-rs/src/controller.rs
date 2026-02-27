//! Controller (server) — listens for SubController connections over WSS.

use crate::certs::{self, CertBundle};
use crate::protocol::*;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::Message;

/// Received message from a peer.
#[derive(Debug, Clone)]
pub enum WireMessage {
    Json {
        peer_fp: String,
        data: Value,
    },
    Binary {
        peer_fp: String,
        data: Vec<u8>,
    },
    File {
        peer_fp: String,
        filename: String,
        data: Vec<u8>,
    },
    Image {
        peer_fp: String,
        filename: String,
        data: Vec<u8>,
    },
}

type PeerSender = mpsc::UnboundedSender<Vec<u8>>;

pub struct Controller {
    host: String,
    port: u16,
    preshared_secret: String,
    cert_bundle: Option<CertBundle>,
    pinned_peers: Arc<RwLock<HashMap<String, bool>>>,
    peer_senders: Arc<RwLock<HashMap<String, PeerSender>>>,
    pub message_rx: Option<mpsc::UnboundedReceiver<WireMessage>>,
    message_tx: mpsc::UnboundedSender<WireMessage>,
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl Controller {
    pub fn new(host: &str, port: u16, preshared_secret: &str) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        Controller {
            host: host.to_string(),
            port,
            preshared_secret: preshared_secret.to_string(),
            cert_bundle: None,
            pinned_peers: Arc::new(RwLock::new(HashMap::new())),
            peer_senders: Arc::new(RwLock::new(HashMap::new())),
            message_rx: Some(message_rx),
            message_tx,
            shutdown_tx: None,
        }
    }

    pub fn fingerprint(&self) -> Option<&str> {
        self.cert_bundle.as_ref().map(|b| b.fingerprint.as_str())
    }

    pub fn cert_bundle(&self) -> Option<&CertBundle> {
        self.cert_bundle.as_ref()
    }

    pub async fn peer_fingerprints(&self) -> Vec<String> {
        self.peer_senders.read().await.keys().cloned().collect()
    }

    /// Start the controller. Returns once the server is listening.
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Generating TLS certificate...");
        let bundle = certs::generate_self_signed_cert("controller")?;
        info!("Controller fingerprint: {}...", &bundle.fingerprint[..16]);

        let tls_config = certs::create_server_config(&bundle)?;
        self.cert_bundle = Some(bundle);

        let acceptor = TlsAcceptor::from(tls_config);
        let listener = TcpListener::bind(format!("{}:{}", self.host, self.port)).await?;
        info!("Controller listening on wss://{}:{}", self.host, self.port);

        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        self.shutdown_tx = Some(shutdown_tx);

        let cert_bundle = self.cert_bundle.clone().unwrap();
        let secret = self.preshared_secret.clone();
        let pinned = self.pinned_peers.clone();
        let senders = self.peer_senders.clone();
        let msg_tx = self.message_tx.clone();

        tokio::spawn(async move {
            let mut shutdown_rx = shutdown_rx;
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        match result {
                            Ok((stream, addr)) => {
                                info!("New connection from {}", addr);
                                let acceptor = acceptor.clone();
                                let bundle = cert_bundle.clone();
                                let secret = secret.clone();
                                let pinned = pinned.clone();
                                let senders = senders.clone();
                                let msg_tx = msg_tx.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_connection(
                                        stream, acceptor, bundle, secret, pinned, senders, msg_tx,
                                    ).await {
                                        error!("Connection error: {}", e);
                                    }
                                });
                            }
                            Err(e) => error!("Accept error: {}", e),
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Controller shutting down.");
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    pub async fn send_json(
        &self,
        peer_fp: &str,
        data: &Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(data)?;
        let frame = encode_frame(MessageType::Json, &payload, None, Flags::NONE, true)?;
        self.send_raw(peer_fp, frame).await
    }

    pub async fn send_binary(
        &self,
        peer_fp: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let frame = encode_frame(MessageType::Binary, data, None, Flags::NONE, true)?;
        self.send_raw(peer_fp, frame).await
    }

    pub async fn send_file(
        &self,
        peer_fp: &str,
        filename: &str,
        data: &[u8],
        is_image: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let msg_type = if is_image {
            MessageType::Image
        } else {
            MessageType::File
        };
        let file_payload = encode_file_payload(filename, data);
        send_streamed(
            &self.peer_senders,
            peer_fp,
            msg_type,
            &file_payload,
        )
        .await
    }

    async fn send_raw(
        &self,
        peer_fp: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let senders = self.peer_senders.read().await;
        let tx = senders
            .get(peer_fp)
            .ok_or_else(|| format!("No peer: {}...", &peer_fp[..16]))?;
        tx.send(data)?;
        Ok(())
    }
}

async fn send_streamed(
    senders: &Arc<RwLock<HashMap<String, PeerSender>>>,
    peer_fp: &str,
    msg_type: MessageType,
    file_payload: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let senders_read = senders.read().await;
    let tx = senders_read
        .get(peer_fp)
        .ok_or_else(|| format!("No peer: {}...", &peer_fp[..16]))?;

    let msg_id = *uuid::Uuid::new_v4().as_bytes();

    if file_payload.len() <= STREAM_CHUNK_SIZE {
        let frame = encode_frame(msg_type, file_payload, Some(msg_id), Flags::NONE, true)?;
        tx.send(frame)?;
        return Ok(());
    }

    let mut offset = 0;
    let mut first = true;
    while offset < file_payload.len() {
        let end = (offset + STREAM_CHUNK_SIZE).min(file_payload.len());
        let chunk = &file_payload[offset..end];
        let is_last = end >= file_payload.len();

        let flags = if first {
            first = false;
            Flags::STREAM_START
        } else if is_last {
            Flags::STREAM_END
        } else {
            Flags::STREAM_CHUNK
        };

        let frame = encode_frame(msg_type, chunk, Some(msg_id), flags, false)?;
        tx.send(frame)?;
        offset = end;
    }
    Ok(())
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    acceptor: TlsAcceptor,
    bundle: CertBundle,
    secret: String,
    pinned: Arc<RwLock<HashMap<String, bool>>>,
    senders: Arc<RwLock<HashMap<String, PeerSender>>>,
    msg_tx: mpsc::UnboundedSender<WireMessage>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tls_stream = acceptor.accept(stream).await?;
    let ws_stream = tokio_tungstenite::accept_async(tls_stream).await?;
    let (mut ws_tx, mut ws_rx) = ws_stream.split();

    // Auth handshake
    let auth_msg = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        ws_rx.next(),
    )
    .await?
    .ok_or("Connection closed during auth")??;

    let auth_raw: Vec<u8> = match &auth_msg {
        Message::Binary(b) => b.to_vec(),
        Message::Text(t) => t.as_bytes().to_vec(),
        _ => return Err("Expected binary auth frame".into()),
    };

    let (header, payload) = decode_frame(&auth_raw)?;
    if header.msg_type != MessageType::Auth {
        return Err(format!("Expected AUTH, got {:?}", header.msg_type).into());
    }

    let auth_data: Value = serde_json::from_slice(&payload)?;
    let peer_secret = auth_data["secret"].as_str().unwrap_or("");
    let peer_fp = auth_data["fingerprint"].as_str().unwrap_or("").to_string();

    if peer_secret != secret {
        warn!("Auth failed: bad secret");
        let fail = encode_frame(
            MessageType::AuthFail,
            br#"{"error":"bad secret"}"#,
            None,
            Flags::NONE,
            false,
        )?;
        ws_tx.send(Message::Binary(fail.into())).await?;
        ws_tx.close().await?;
        return Ok(());
    }

    {
        let mut pinned = pinned.write().await;
        if pinned.contains_key(&peer_fp) {
            info!("Known peer reconnecting: {}...", &peer_fp[..16]);
        } else {
            info!("Pinning new peer: {}...", &peer_fp[..16]);
            pinned.insert(peer_fp.clone(), true);
        }
    }

    let ok_data = serde_json::json!({
        "cert_pem": bundle.cert_pem,
        "fingerprint": bundle.fingerprint,
    });
    let ok_frame = encode_frame(
        MessageType::AuthOk,
        serde_json::to_vec(&ok_data)?.as_slice(),
        None,
        Flags::NONE,
        false,
    )?;
    ws_tx.send(Message::Binary(ok_frame.into())).await?;

    // Set up send channel for this peer
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    {
        senders.write().await.insert(peer_fp.clone(), peer_tx);
    }

    // Stream reassembly state
    let stream_buffers: Arc<Mutex<HashMap<[u8; 16], Vec<Vec<u8>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let stream_meta: Arc<Mutex<HashMap<[u8; 16], MessageType>>> =
        Arc::new(Mutex::new(HashMap::new()));

    let peer_fp_clone = peer_fp.clone();
    let senders_clone = senders.clone();

    // Spawn writer
    let write_handle = tokio::spawn(async move {
        while let Some(data) = peer_rx.recv().await {
            if ws_tx.send(Message::Binary(data.into())).await.is_err() {
                break;
            }
        }
    });

    // Read loop
    while let Some(msg) = ws_rx.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("Read error: {}", e);
                break;
            }
        };

        let raw = match &msg {
            Message::Binary(b) => b.to_vec(),
            Message::Text(t) => t.as_bytes().to_vec(),
            Message::Close(_) => break,
            _ => continue,
        };

        let (header, payload) = match decode_frame(&raw) {
            Ok(r) => r,
            Err(e) => {
                error!("Frame decode error: {}", e);
                continue;
            }
        };

        // Stream reassembly
        if header.flags.intersects(Flags::STREAM_START | Flags::STREAM_CHUNK | Flags::STREAM_END) {
            let mut bufs = stream_buffers.lock().await;
            let mut meta = stream_meta.lock().await;

            if header.flags.contains(Flags::STREAM_START) {
                bufs.insert(header.msg_id, vec![payload.clone()]);
                meta.insert(header.msg_id, header.msg_type);
            } else if header.flags.contains(Flags::STREAM_CHUNK) {
                if let Some(buf) = bufs.get_mut(&header.msg_id) {
                    buf.push(payload.clone());
                }
            }
            if header.flags.contains(Flags::STREAM_END) {
                if let Some(buf) = bufs.get_mut(&header.msg_id) {
                    buf.push(payload);
                }
                if let Some(chunks) = bufs.remove(&header.msg_id) {
                    let full: Vec<u8> = chunks.into_iter().flatten().collect();
                    let mt = meta.remove(&header.msg_id).unwrap_or(header.msg_type);
                    dispatch_message(&peer_fp, mt, &full, &msg_tx);
                }
            }
            continue;
        }

        dispatch_message(&peer_fp, header.msg_type, &payload, &msg_tx);
    }

    write_handle.abort();
    senders_clone.write().await.remove(&peer_fp_clone);
    info!("Peer disconnected: {}...", &peer_fp_clone[..16.min(peer_fp_clone.len())]);
    Ok(())
}

fn dispatch_message(
    peer_fp: &str,
    msg_type: MessageType,
    payload: &[u8],
    msg_tx: &mpsc::UnboundedSender<WireMessage>,
) {
    let wire_msg = match msg_type {
        MessageType::Json => {
            if let Ok(data) = serde_json::from_slice(payload) {
                WireMessage::Json {
                    peer_fp: peer_fp.to_string(),
                    data,
                }
            } else {
                return;
            }
        }
        MessageType::Binary => WireMessage::Binary {
            peer_fp: peer_fp.to_string(),
            data: payload.to_vec(),
        },
        MessageType::File => {
            if let Ok((filename, data)) = decode_file_payload(payload) {
                WireMessage::File {
                    peer_fp: peer_fp.to_string(),
                    filename,
                    data,
                }
            } else {
                return;
            }
        }
        MessageType::Image => {
            if let Ok((filename, data)) = decode_file_payload(payload) {
                WireMessage::Image {
                    peer_fp: peer_fp.to_string(),
                    filename,
                    data,
                }
            } else {
                return;
            }
        }
        _ => return,
    };
    let _ = msg_tx.send(wire_msg);
}
