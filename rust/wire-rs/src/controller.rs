//! Controller (server) — listens for SubController connections over WSS.
//! Supports relay between SubControllers and peer discovery notifications.

use crate::certs::{self, CertBundle};
use crate::protocol::*;
use crate::proxy::ReverseProxy;
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
    // Embedded reverse proxy
    proxy: Option<ReverseProxy>,
    /// Maps peer fingerprint → list of path prefixes bound to that peer.
    peer_proxy_routes: Arc<RwLock<HashMap<String, Vec<String>>>>,
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
            proxy: None,
            peer_proxy_routes: Arc::new(RwLock::new(HashMap::new())),
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
        let peer_proxy_routes = self.peer_proxy_routes.clone();
        let proxy_routes_table = self.proxy.as_ref().map(|p| p.routes_table());

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
                                let peer_proxy_routes = peer_proxy_routes.clone();
                                let proxy_routes_table = proxy_routes_table.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = handle_connection(
                                        stream, acceptor, bundle, secret, pinned, senders, msg_tx,
                                        peer_proxy_routes, proxy_routes_table,
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
        if let Some(proxy) = self.proxy.as_mut() {
            proxy.stop().await;
        }
        self.proxy = None;
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(true);
        }
    }

    // -- embedded reverse proxy -----------------------------------------------

    /// Start an embedded HTTP reverse proxy alongside the WebSocket server.
    pub async fn enable_proxy(
        &mut self,
        host: &str,
        port: u16,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut proxy = ReverseProxy::new(host, port);
        proxy.start().await?;
        self.proxy = Some(proxy);
        info!("Embedded proxy enabled on http://{}:{}", host, port);
        Ok(())
    }

    /// Stop the embedded proxy.
    pub async fn disable_proxy(&mut self) {
        if let Some(mut proxy) = self.proxy.take() {
            proxy.stop().await;
        }
        self.peer_proxy_routes.write().await.clear();
        info!("Embedded proxy disabled.");
    }

    /// Add a static proxy route (not tied to any peer lifecycle).
    pub async fn add_proxy_route(&self, path_prefix: &str, upstream_url: &str) {
        let proxy = self.proxy.as_ref().expect("Proxy not enabled. Call enable_proxy() first.");
        proxy.add_route(path_prefix, upstream_url).await;
    }

    /// Remove a proxy route.
    pub async fn remove_proxy_route(&self, path_prefix: &str) {
        let proxy = self.proxy.as_ref().expect("Proxy not enabled. Call enable_proxy() first.");
        proxy.remove_route(path_prefix).await;
    }

    /// Add a proxy route tied to a connected peer's lifecycle.
    ///
    /// When the peer disconnects, the route is automatically removed.
    pub async fn add_proxy_route_for_peer(
        &self,
        path_prefix: &str,
        peer_fp: &str,
        upstream_url: &str,
    ) {
        let proxy = self.proxy.as_ref().expect("Proxy not enabled. Call enable_proxy() first.");
        proxy.add_route(path_prefix, upstream_url).await;
        self.peer_proxy_routes
            .write()
            .await
            .entry(peer_fp.to_string())
            .or_default()
            .push(path_prefix.to_string());
        info!(
            "Proxy route {} -> {} bound to peer {}...",
            path_prefix,
            upstream_url,
            &peer_fp[..16.min(peer_fp.len())]
        );
    }

    /// Return a snapshot of the current proxy routes, or None if proxy is not enabled.
    pub async fn proxy_routes(&self) -> Option<HashMap<String, String>> {
        match self.proxy.as_ref() {
            Some(proxy) => Some(proxy.routes_snapshot().await),
            None => None,
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

/// Send a payload using streaming chunks if needed.
fn send_payload_via_tx(
    tx: &PeerSender,
    msg_type: MessageType,
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    send_payload_via_tx(tx, msg_type, file_payload)
}

/// Send a peer event notification (JSON) to a single peer.
fn send_peer_event(
    tx: &PeerSender,
    event: &str,
    peer_fp: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let data = serde_json::json!({"_wire_peer_event": event, "peer_fp": peer_fp});
    let payload = serde_json::to_vec(&data)?;
    let frame = encode_frame(MessageType::Json, &payload, None, Flags::NONE, true)?;
    tx.send(frame)?;
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
    peer_proxy_routes: Arc<RwLock<HashMap<String, Vec<String>>>>,
    proxy_routes_table: Option<crate::proxy::RouteTable>,
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

    // Peer notifications: tell existing peers about the new peer, and vice versa
    {
        let mut senders_write = senders.write().await;

        // Tell the new peer about all existing peers
        for existing_fp in senders_write.keys() {
            let _ = send_peer_event(&peer_tx, "joined", existing_fp);
        }

        // Tell all existing peers about the new peer
        for (_, existing_tx) in senders_write.iter() {
            let _ = send_peer_event(existing_tx, "joined", &peer_fp);
        }

        senders_write.insert(peer_fp.clone(), peer_tx);
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

                    // Relay messages need special handling
                    if mt == MessageType::Relay {
                        handle_relay(&peer_fp, &full, &senders).await;
                    } else if mt == MessageType::Json {
                        // Check for proxy route request in streamed JSON
                        if let Ok(data) = serde_json::from_slice::<Value>(&full) {
                            if data.get("_wire_proxy_route").is_some() {
                                handle_proxy_route_request(
                                    &peer_fp,
                                    &data["_wire_proxy_route"],
                                    &peer_proxy_routes,
                                    &proxy_routes_table,
                                    &senders,
                                )
                                .await;
                            } else {
                                dispatch_message(&peer_fp, mt, &full, &msg_tx);
                            }
                        } else {
                            dispatch_message(&peer_fp, mt, &full, &msg_tx);
                        }
                    } else {
                        dispatch_message(&peer_fp, mt, &full, &msg_tx);
                    }
                }
            }
            continue;
        }

        // Non-streamed relay
        if header.msg_type == MessageType::Relay {
            handle_relay(&peer_fp, &payload, &senders).await;
            continue;
        }

        // Intercept _wire_proxy_route requests (built-in protocol)
        if header.msg_type == MessageType::Json {
            if let Ok(data) = serde_json::from_slice::<Value>(&payload) {
                if data.get("_wire_proxy_route").is_some() {
                    handle_proxy_route_request(
                        &peer_fp,
                        &data["_wire_proxy_route"],
                        &peer_proxy_routes,
                        &proxy_routes_table,
                        &senders,
                    )
                    .await;
                    continue;
                }
            }
        }

        dispatch_message(&peer_fp, header.msg_type, &payload, &msg_tx);
    }

    write_handle.abort();

    // Notify remaining peers that this peer has left
    {
        let mut senders_write = senders_clone.write().await;
        senders_write.remove(&peer_fp_clone);
        for (_, tx) in senders_write.iter() {
            let _ = send_peer_event(tx, "left", &peer_fp_clone);
        }
    }

    // Clean up proxy routes bound to this peer
    if let Some(ref proxy_table) = proxy_routes_table {
        let mut ppr = peer_proxy_routes.write().await;
        if let Some(prefixes) = ppr.remove(&peer_fp_clone) {
            let mut routes = proxy_table.write().await;
            for prefix in &prefixes {
                let normalised = if prefix == "/" {
                    "/".to_string()
                } else {
                    format!("/{}", prefix.trim_matches('/'))
                };
                routes.remove(&normalised);
                info!(
                    "Auto-removed proxy route {} (peer {}... left)",
                    normalised,
                    &peer_fp_clone[..16.min(peer_fp_clone.len())]
                );
            }
        }
    }

    info!("Peer disconnected: {}...", &peer_fp_clone[..16.min(peer_fp_clone.len())]);
    Ok(())
}

/// Handle a relay message: decode, find destination, re-wrap with actual sender, forward.
async fn handle_relay(
    sender_fp: &str,
    payload: &[u8],
    senders: &Arc<RwLock<HashMap<String, PeerSender>>>,
) {
    let (_source_fp, dest_fp, inner_type, inner_payload) = match decode_relay_payload(payload) {
        Ok(r) => r,
        Err(e) => {
            error!("Relay decode error: {}", e);
            return;
        }
    };

    let senders_read = senders.read().await;
    let dest_tx = match senders_read.get(&dest_fp) {
        Some(tx) => tx,
        None => {
            warn!("Relay target not connected: {}...", &dest_fp[..16.min(dest_fp.len())]);
            return;
        }
    };

    // Re-wrap with the actual sender fingerprint (don't trust client-supplied source)
    let relay_out = encode_relay_payload(sender_fp, &dest_fp, inner_type, &inner_payload);
    if let Err(e) = send_payload_via_tx(dest_tx, MessageType::Relay, &relay_out) {
        error!("Relay forward error: {}", e);
    }
}

/// Handle a `_wire_proxy_route` request from a SubController.
///
/// This is part of the built-in Wire protocol — SubControllers can configure
/// the Controller's reverse proxy without custom handler code.
async fn handle_proxy_route_request(
    requester_fp: &str,
    route_cfg: &Value,
    peer_proxy_routes: &Arc<RwLock<HashMap<String, Vec<String>>>>,
    proxy_routes_table: &Option<crate::proxy::RouteTable>,
    senders: &Arc<RwLock<HashMap<String, PeerSender>>>,
) {
    let action = route_cfg["action"].as_str().unwrap_or("");
    let path_prefix = route_cfg["path_prefix"].as_str().unwrap_or("");

    let send_result = |result: Value| {
        let senders = senders.clone();
        let fp = requester_fp.to_string();
        async move {
            let payload = serde_json::to_vec(&result).unwrap_or_default();
            if let Ok(frame) = encode_frame(MessageType::Json, &payload, None, Flags::NONE, true) {
                let senders_read = senders.read().await;
                if let Some(tx) = senders_read.get(&fp) {
                    let _ = tx.send(frame);
                }
            }
        }
    };

    match action {
        "add" => {
            let peer_fp = route_cfg["peer_fp"]
                .as_str()
                .unwrap_or(requester_fp);
            let upstream_url = route_cfg["upstream_url"].as_str().unwrap_or("");

            let proxy_table = match proxy_routes_table {
                Some(t) => t,
                None => {
                    warn!(
                        "Proxy route request from {}... rejected: proxy not enabled",
                        &requester_fp[..16.min(requester_fp.len())]
                    );
                    send_result(serde_json::json!({
                        "_wire_proxy_route_result": {
                            "ok": false,
                            "error": "Proxy not enabled on controller",
                            "path_prefix": path_prefix,
                        }
                    }))
                    .await;
                    return;
                }
            };

            // Normalise and insert the route
            let normalised = if path_prefix == "/" || path_prefix.is_empty() {
                "/".to_string()
            } else {
                format!("/{}", path_prefix.trim_matches('/'))
            };
            let upstream = upstream_url.trim_end_matches('/').to_string();

            proxy_table
                .write()
                .await
                .insert(normalised.clone(), upstream.clone());

            // Track as peer-bound route
            peer_proxy_routes
                .write()
                .await
                .entry(peer_fp.to_string())
                .or_default()
                .push(normalised.clone());

            info!(
                "Proxy route {} -> {} bound to peer {}... (requested by {}...)",
                normalised,
                upstream,
                &peer_fp[..16.min(peer_fp.len())],
                &requester_fp[..16.min(requester_fp.len())]
            );

            send_result(serde_json::json!({
                "_wire_proxy_route_result": {
                    "ok": true,
                    "action": "add",
                    "path_prefix": normalised,
                    "upstream_url": upstream,
                    "peer_fp": peer_fp,
                }
            }))
            .await;
        }
        "remove" => {
            let proxy_table = match proxy_routes_table {
                Some(t) => t,
                None => {
                    send_result(serde_json::json!({
                        "_wire_proxy_route_result": {
                            "ok": false,
                            "error": "Proxy not enabled on controller",
                            "path_prefix": path_prefix,
                        }
                    }))
                    .await;
                    return;
                }
            };

            let normalised = if path_prefix == "/" || path_prefix.is_empty() {
                "/".to_string()
            } else {
                format!("/{}", path_prefix.trim_matches('/'))
            };

            proxy_table.write().await.remove(&normalised);

            // Remove from peer-bound tracking
            let mut ppr = peer_proxy_routes.write().await;
            for prefixes in ppr.values_mut() {
                prefixes.retain(|p| p != &normalised);
            }

            info!(
                "Proxy route {} removed (requested by {}...)",
                normalised,
                &requester_fp[..16.min(requester_fp.len())]
            );

            send_result(serde_json::json!({
                "_wire_proxy_route_result": {
                    "ok": true,
                    "action": "remove",
                    "path_prefix": normalised,
                }
            }))
            .await;
        }
        _ => {
            warn!("Unknown proxy route action: {}", action);
        }
    }
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
