//! SubController (client) — connects to a Controller over WSS.
//! Supports peer-to-peer relay via Controller and automatic peer discovery.

use crate::certs::{self, CertBundle};
use crate::protocol::*;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;

/// Received message (from controller or relayed from another peer).
#[derive(Debug, Clone)]
pub enum WireMessage {
    Json { data: Value },
    Binary { data: Vec<u8> },
    File { filename: String, data: Vec<u8> },
    Image { filename: String, data: Vec<u8> },
    /// A message relayed from another SubController.
    RelayJson { source_fp: String, data: Value },
    RelayBinary { source_fp: String, data: Vec<u8> },
    RelayFile { source_fp: String, filename: String, data: Vec<u8> },
    RelayImage { source_fp: String, filename: String, data: Vec<u8> },
    /// Peer discovery events.
    PeerJoined { peer_fp: String },
    PeerLeft { peer_fp: String },
}

pub struct SubController {
    controller_host: String,
    controller_port: u16,
    preshared_secret: String,
    cert_bundle: Option<CertBundle>,
    controller_fingerprint: Arc<RwLock<Option<String>>>,
    sender: Arc<RwLock<Option<mpsc::UnboundedSender<Vec<u8>>>>>,
    pub message_rx: Option<mpsc::UnboundedReceiver<WireMessage>>,
    message_tx: mpsc::UnboundedSender<WireMessage>,
    listen_handle: Option<tokio::task::JoinHandle<()>>,
    known_peers: Arc<RwLock<HashSet<String>>>,
}

impl SubController {
    pub fn new(host: &str, port: u16, preshared_secret: &str) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();
        SubController {
            controller_host: host.to_string(),
            controller_port: port,
            preshared_secret: preshared_secret.to_string(),
            cert_bundle: None,
            controller_fingerprint: Arc::new(RwLock::new(None)),
            sender: Arc::new(RwLock::new(None)),
            message_rx: Some(message_rx),
            message_tx,
            listen_handle: None,
            known_peers: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn fingerprint(&self) -> Option<String> {
        self.cert_bundle.as_ref().map(|b| b.fingerprint.clone())
    }

    pub fn cert_bundle(&self) -> Option<&CertBundle> {
        self.cert_bundle.as_ref()
    }

    pub async fn controller_fingerprint(&self) -> Option<String> {
        self.controller_fingerprint.read().await.clone()
    }

    pub async fn known_peers(&self) -> Vec<String> {
        self.known_peers.read().await.iter().cloned().collect()
    }

    pub async fn connect(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Generating TLS certificate...");
        let bundle = certs::generate_self_signed_cert("subcontroller")?;
        info!("SubController fingerprint: {}...", &bundle.fingerprint[..16]);

        let tls_config = certs::create_client_config(&bundle)?;
        self.cert_bundle = Some(bundle.clone());

        let connector = tokio_rustls::TlsConnector::from(tls_config);
        let tcp = tokio::net::TcpStream::connect(format!(
            "{}:{}",
            self.controller_host, self.controller_port
        ))
        .await?;

        let server_name = rustls::pki_types::ServerName::try_from("localhost")?;
        let tls_stream = connector.connect(server_name, tcp).await?;

        let url = format!(
            "wss://{}:{}/",
            self.controller_host, self.controller_port
        );
        let (ws_stream, _) =
            tokio_tungstenite::client_async(url, tls_stream).await?;
        let (mut ws_tx, mut ws_rx) = ws_stream.split();

        // Auth handshake
        let auth_data = serde_json::json!({
            "secret": self.preshared_secret,
            "cert_pem": bundle.cert_pem,
            "fingerprint": bundle.fingerprint,
        });
        let auth_frame = encode_frame(
            MessageType::Auth,
            serde_json::to_vec(&auth_data)?.as_slice(),
            None,
            Flags::NONE,
            false,
        )?;
        ws_tx.send(Message::Binary(auth_frame.into())).await?;

        // Wait for response
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            ws_rx.next(),
        )
        .await?
        .ok_or("Connection closed during auth")??;

        let resp_raw = match &resp {
            Message::Binary(b) => b.to_vec(),
            Message::Text(t) => t.as_bytes().to_vec(),
            _ => return Err("Unexpected auth response type".into()),
        };

        let (header, payload) = decode_frame(&resp_raw)?;
        match header.msg_type {
            MessageType::AuthFail => {
                let err: Value = serde_json::from_slice(&payload)?;
                return Err(format!(
                    "Authentication failed: {}",
                    err["error"].as_str().unwrap_or("unknown")
                )
                .into());
            }
            MessageType::AuthOk => {}
            _ => {
                return Err(format!("Unexpected auth response: {:?}", header.msg_type).into());
            }
        }

        let ok_data: Value = serde_json::from_slice(&payload)?;
        let ctrl_fp = ok_data["fingerprint"]
            .as_str()
            .unwrap_or("")
            .to_string();

        {
            let mut fp = self.controller_fingerprint.write().await;
            if let Some(existing) = fp.as_ref() {
                if existing != &ctrl_fp {
                    return Err(format!(
                        "Controller fingerprint mismatch! Expected {}..., got {}...",
                        &existing[..16],
                        &ctrl_fp[..16]
                    )
                    .into());
                }
            } else {
                info!("Pinned controller fingerprint: {}...", &ctrl_fp[..16]);
                *fp = Some(ctrl_fp);
            }
        }

        // Set up send channel
        let (send_tx, mut send_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        *self.sender.write().await = Some(send_tx);

        // Spawn writer
        let write_handle = tokio::spawn(async move {
            while let Some(data) = send_rx.recv().await {
                if ws_tx.send(Message::Binary(data.into())).await.is_err() {
                    break;
                }
            }
        });

        // Spawn reader
        let msg_tx = self.message_tx.clone();
        let known_peers = self.known_peers.clone();
        let listen_handle = tokio::spawn(async move {
            let stream_buffers: HashMap<[u8; 16], Vec<Vec<u8>>> = HashMap::new();
            let stream_meta: HashMap<[u8; 16], MessageType> = HashMap::new();
            let bufs = Arc::new(Mutex::new(stream_buffers));
            let meta = Arc::new(Mutex::new(stream_meta));

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

                if header
                    .flags
                    .intersects(Flags::STREAM_START | Flags::STREAM_CHUNK | Flags::STREAM_END)
                {
                    let mut b = bufs.lock().await;
                    let mut m = meta.lock().await;

                    if header.flags.contains(Flags::STREAM_START) {
                        b.insert(header.msg_id, vec![payload.clone()]);
                        m.insert(header.msg_id, header.msg_type);
                    } else if header.flags.contains(Flags::STREAM_CHUNK) {
                        if let Some(buf) = b.get_mut(&header.msg_id) {
                            buf.push(payload.clone());
                        }
                    }
                    if header.flags.contains(Flags::STREAM_END) {
                        if let Some(buf) = b.get_mut(&header.msg_id) {
                            buf.push(payload);
                        }
                        if let Some(chunks) = b.remove(&header.msg_id) {
                            let full: Vec<u8> = chunks.into_iter().flatten().collect();
                            let mt = m.remove(&header.msg_id).unwrap_or(header.msg_type);

                            if mt == MessageType::Relay {
                                dispatch_relay(&full, &msg_tx);
                            } else {
                                dispatch_sub_message(mt, &full, &msg_tx, &known_peers).await;
                            }
                        }
                    }
                    continue;
                }

                // Non-streamed relay
                if header.msg_type == MessageType::Relay {
                    dispatch_relay(&payload, &msg_tx);
                    continue;
                }

                dispatch_sub_message(header.msg_type, &payload, &msg_tx, &known_peers).await;
            }
            write_handle.abort();
        });

        self.listen_handle = Some(listen_handle);
        info!("Connected and authenticated to controller.");
        Ok(())
    }

    pub async fn disconnect(&mut self) {
        if let Some(h) = self.listen_handle.take() {
            h.abort();
        }
        *self.sender.write().await = None;
        info!("Disconnected from controller.");
    }

    pub async fn send_json(
        &self,
        data: &Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(data)?;
        let frame = encode_frame(MessageType::Json, &payload, None, Flags::NONE, true)?;
        self.send_raw(frame).await
    }

    pub async fn send_binary(
        &self,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let frame = encode_frame(MessageType::Binary, data, None, Flags::NONE, true)?;
        self.send_raw(frame).await
    }

    pub async fn send_file(
        &self,
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
        self.send_payload_streamed(msg_type, &file_payload).await
    }

    // -- peer-to-peer via relay -----------------------------------------------

    pub async fn send_json_to_peer(
        &self,
        dest_fp: &str,
        data: &Value,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let inner = serde_json::to_vec(data)?;
        let fp = self.fingerprint().ok_or("No fingerprint")?;
        let relay = encode_relay_payload(&fp, dest_fp, MessageType::Json, &inner);
        self.send_payload_streamed(MessageType::Relay, &relay).await
    }

    pub async fn send_binary_to_peer(
        &self,
        dest_fp: &str,
        data: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let fp = self.fingerprint().ok_or("No fingerprint")?;
        let relay = encode_relay_payload(&fp, dest_fp, MessageType::Binary, data);
        self.send_payload_streamed(MessageType::Relay, &relay).await
    }

    pub async fn send_file_to_peer(
        &self,
        dest_fp: &str,
        filename: &str,
        data: &[u8],
        is_image: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let inner_type = if is_image {
            MessageType::Image
        } else {
            MessageType::File
        };
        let inner = encode_file_payload(filename, data);
        let fp = self.fingerprint().ok_or("No fingerprint")?;
        let relay = encode_relay_payload(&fp, dest_fp, inner_type, &inner);
        self.send_payload_streamed(MessageType::Relay, &relay).await
    }

    // -- internal -------------------------------------------------------------

    async fn send_raw(
        &self,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sender = self.sender.read().await;
        let tx = sender.as_ref().ok_or("Not connected")?;
        tx.send(data)?;
        Ok(())
    }

    async fn send_payload_streamed(
        &self,
        msg_type: MessageType,
        payload: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let sender = self.sender.read().await;
        let tx = sender.as_ref().ok_or("Not connected")?;
        let msg_id = *uuid::Uuid::new_v4().as_bytes();

        if payload.len() <= STREAM_CHUNK_SIZE {
            let frame = encode_frame(msg_type, payload, Some(msg_id), Flags::NONE, true)?;
            tx.send(frame)?;
            return Ok(());
        }

        let mut offset = 0;
        let mut first = true;
        while offset < payload.len() {
            let end = (offset + STREAM_CHUNK_SIZE).min(payload.len());
            let chunk = &payload[offset..end];
            let is_last = end >= payload.len();

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
}

/// Dispatch a relay message to the message channel.
fn dispatch_relay(
    payload: &[u8],
    msg_tx: &mpsc::UnboundedSender<WireMessage>,
) {
    let (source_fp, _dest_fp, inner_type, inner_payload) = match decode_relay_payload(payload) {
        Ok(r) => r,
        Err(e) => {
            error!("Relay decode error: {}", e);
            return;
        }
    };

    let wire_msg = match inner_type {
        MessageType::Json => {
            if let Ok(data) = serde_json::from_slice(&inner_payload) {
                WireMessage::RelayJson { source_fp, data }
            } else {
                return;
            }
        }
        MessageType::Binary => WireMessage::RelayBinary {
            source_fp,
            data: inner_payload,
        },
        MessageType::File => {
            if let Ok((filename, data)) = decode_file_payload(&inner_payload) {
                WireMessage::RelayFile {
                    source_fp,
                    filename,
                    data,
                }
            } else {
                return;
            }
        }
        MessageType::Image => {
            if let Ok((filename, data)) = decode_file_payload(&inner_payload) {
                WireMessage::RelayImage {
                    source_fp,
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

async fn dispatch_sub_message(
    msg_type: MessageType,
    payload: &[u8],
    msg_tx: &mpsc::UnboundedSender<WireMessage>,
    known_peers: &Arc<RwLock<HashSet<String>>>,
) {
    // Intercept peer events
    if msg_type == MessageType::Json {
        if let Ok(data) = serde_json::from_slice::<Value>(payload) {
            if let Some(event) = data.get("_wire_peer_event").and_then(|v| v.as_str()) {
                if let Some(peer_fp) = data.get("peer_fp").and_then(|v| v.as_str()) {
                    let peer_fp = peer_fp.to_string();
                    match event {
                        "joined" => {
                            known_peers.write().await.insert(peer_fp.clone());
                            let _ = msg_tx.send(WireMessage::PeerJoined { peer_fp });
                        }
                        "left" => {
                            known_peers.write().await.remove(&peer_fp);
                            let _ = msg_tx.send(WireMessage::PeerLeft { peer_fp });
                        }
                        _ => {}
                    }
                }
                return;
            }
            // Regular JSON - pass through
            let _ = msg_tx.send(WireMessage::Json { data });
            return;
        }
        return;
    }

    let wire_msg = match msg_type {
        MessageType::Binary => WireMessage::Binary {
            data: payload.to_vec(),
        },
        MessageType::File => {
            if let Ok((filename, data)) = decode_file_payload(payload) {
                WireMessage::File { filename, data }
            } else {
                return;
            }
        }
        MessageType::Image => {
            if let Ok((filename, data)) = decode_file_payload(payload) {
                WireMessage::Image { filename, data }
            } else {
                return;
            }
        }
        _ => return,
    };
    let _ = msg_tx.send(wire_msg);
}
