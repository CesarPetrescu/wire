/**
 * Controller (server) — listens for SubController connections over WSS.
 *
 * Mirrors the Python/Rust Controller:
 *   - Self-signed TLS cert on startup
 *   - Pre-shared secret authentication
 *   - Cert fingerprint pinning
 *   - JSON / Binary / File / Image / Streaming
 *   - Relay: forwards messages between SubControllers (star topology)
 *   - Peer notifications: join/leave events
 *   - HTTP tunnel: forwards requests to SubControllers serving HTTP
 */

import { EventEmitter } from "events";
import * as https from "https";
import * as tls from "tls";
import WebSocket, { WebSocketServer } from "ws";
import { CertBundle, generateSelfSignedCert } from "./certs";
import {
  STREAM_CHUNK_SIZE,
  MessageType,
  Flags,
  encodeFrame,
  decodeFrame,
  encodeFilePayload,
  decodeFilePayload,
  encodeRelayPayload,
  decodeRelayPayload,
  encodeHttpRequest,
  decodeHttpRequest,
  decodeHttpResponse,
  FrameHeader,
  HttpMethod,
  httpMethodFromStr,
} from "./protocol";
import { randomUUID } from "crypto";

export interface TunnelRoute {
  prefix: string;
  upstream: string;
  peer_fp: string;
  health_check?: string;
  healthy: boolean;
}

export type MessageHandler = (...args: any[]) => void | Promise<void>;

export class Controller extends EventEmitter {
  readonly host: string;
  readonly port: number;
  readonly presharedSecret: string;

  certBundle: CertBundle | null = null;

  private _wss: WebSocketServer | null = null;
  private _httpsServer: https.Server | null = null;
  private _pinnedPeers: Map<string, boolean> = new Map();
  private _peers: Map<string, WebSocket> = new Map();
  private _handlers: Map<number, MessageHandler> = new Map();

  // Stream reassembly
  private _streamBuffers: Map<string, Buffer[]> = new Map();
  private _streamMeta: Map<string, MessageType> = new Map();

  // Tunnel routes
  private _tunnelRoutes: Map<string, TunnelRoute> = new Map();
  // Pending HTTP tunnel requests: msgId hex -> {resolve, reject}
  private _pendingRequests: Map<string, { resolve: (payload: Buffer) => void; reject: (err: Error) => void }> = new Map();

  constructor(host: string = "0.0.0.0", port: number = 8765, presharedSecret: string = "") {
    super();
    this.host = host;
    this.port = port;
    this.presharedSecret = presharedSecret;
  }

  get fingerprint(): string | null {
    return this.certBundle?.fingerprint ?? null;
  }

  get peerFingerprints(): string[] {
    return Array.from(this._peers.keys());
  }

  get tunnelRoutes(): Map<string, TunnelRoute> {
    return new Map(this._tunnelRoutes);
  }

  /**
   * Register a handler for a message type.
   */
  onMessage(msgType: MessageType, handler: MessageHandler): void {
    this._handlers.set(msgType, handler);
  }

  // ── Lifecycle ─────────────────────────────────────────────────────────────

  async start(): Promise<void> {
    this.certBundle = generateSelfSignedCert("controller");

    this._httpsServer = https.createServer({
      cert: this.certBundle.certPem,
      key: this.certBundle.keyPem,
      requestCert: false,
      rejectUnauthorized: false,
    });

    this._wss = new WebSocketServer({ server: this._httpsServer });

    this._wss.on("connection", (ws: WebSocket) => {
      this._handleConnection(ws);
    });

    await new Promise<void>((resolve) => {
      this._httpsServer!.listen(this.port, this.host, () => resolve());
    });
  }

  async stop(): Promise<void> {
    // Close all peer connections
    for (const ws of this._peers.values()) {
      ws.close();
    }
    this._peers.clear();

    if (this._wss) {
      this._wss.close();
      this._wss = null;
    }
    if (this._httpsServer) {
      await new Promise<void>((resolve) => {
        this._httpsServer!.close(() => resolve());
      });
      this._httpsServer = null;
    }
  }

  // ── Send methods ──────────────────────────────────────────────────────────

  async sendJson(peerFp: string, data: any): Promise<void> {
    const ws = this._peers.get(peerFp);
    if (!ws) throw new Error(`No peer with fingerprint ${peerFp.substring(0, 16)}...`);
    const payload = Buffer.from(JSON.stringify(data), "utf-8");
    const frame = encodeFrame(MessageType.JSON, payload, undefined, Flags.NONE, true);
    ws.send(frame);
  }

  async sendBinary(peerFp: string, data: Buffer): Promise<void> {
    const ws = this._peers.get(peerFp);
    if (!ws) throw new Error(`No peer with fingerprint ${peerFp.substring(0, 16)}...`);
    const frame = encodeFrame(MessageType.BINARY, data, undefined, Flags.NONE, true);
    ws.send(frame);
  }

  async sendFile(peerFp: string, filename: string, data: Buffer, isImage = false): Promise<void> {
    const ws = this._peers.get(peerFp);
    if (!ws) throw new Error(`No peer with fingerprint ${peerFp.substring(0, 16)}...`);
    const msgType = isImage ? MessageType.IMAGE : MessageType.FILE;
    const filePayload = encodeFilePayload(filename, data);
    this._sendPayloadStreamed(ws, msgType, filePayload);
  }

  async broadcastJson(data: any): Promise<void> {
    const payload = Buffer.from(JSON.stringify(data), "utf-8");
    const frame = encodeFrame(MessageType.JSON, payload, undefined, Flags.NONE, true);
    for (const ws of this._peers.values()) {
      ws.send(frame);
    }
  }

  // ── Tunnel ────────────────────────────────────────────────────────────────

  registerPeerRoutes(peerFp: string, services: any[]): void {
    for (const svc of services) {
      const prefix = "/" + (svc.prefix || "").replace(/^\/|\/$/g, "");
      const route: TunnelRoute = {
        prefix,
        upstream: svc.upstream,
        peer_fp: peerFp,
        health_check: svc.health_check,
        healthy: true,
      };
      this._tunnelRoutes.set(prefix, route);
    }
  }

  deregisterPeerRoutes(peerFp: string): void {
    for (const [prefix, route] of this._tunnelRoutes) {
      if (route.peer_fp === peerFp) {
        this._tunnelRoutes.delete(prefix);
      }
    }
  }

  async tunnelRequest(
    peerFp: string,
    method: string,
    path: string,
    query: string,
    headers: [string, string][],
    body: Buffer,
    timeout: number = 30000,
  ): Promise<[number, [string, string][], Buffer]> {
    const ws = this._peers.get(peerFp);
    if (!ws) return [502, [], Buffer.from("Peer not connected")];

    const httpMethod = httpMethodFromStr(method);
    const reqPayload = encodeHttpRequest(httpMethod, path, query, headers, body);
    const msgId = Buffer.from(randomUUID().replace(/-/g, ""), "hex");
    const midKey = msgId.toString("hex");

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pendingRequests.delete(midKey);
        resolve([504, [], Buffer.from("Gateway Timeout")]);
      }, timeout);

      this._pendingRequests.set(midKey, {
        resolve: (respPayload: Buffer) => {
          clearTimeout(timer);
          this._pendingRequests.delete(midKey);
          try {
            resolve(decodeHttpResponse(respPayload));
          } catch (err) {
            resolve([502, [], Buffer.from("Bad Gateway")]);
          }
        },
        reject: (err: Error) => {
          clearTimeout(timer);
          this._pendingRequests.delete(midKey);
          resolve([502, [], Buffer.from("Bad Gateway")]);
        },
      });

      const frame = encodeFrame(MessageType.HTTP_REQUEST, reqPayload, msgId, Flags.NONE, true);
      ws.send(frame);
    });
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private _handleConnection(ws: WebSocket): void {
    let peerFp: string | null = null;

    // Wait for first message (AUTH)
    const authTimeout = setTimeout(() => {
      ws.close();
    }, 10000);

    ws.once("message", (raw: Buffer) => {
      clearTimeout(authTimeout);

      try {
        peerFp = this._authenticate(ws, Buffer.from(raw));
        if (!peerFp) return;

        this._peers.set(peerFp, ws);
        this._notifyPeerJoined(peerFp);

        // Set up message loop
        const fp = peerFp;
        ws.on("message", (data: Buffer) => {
          try {
            this._dispatch(fp, Buffer.from(data));
          } catch (err) {
            this.emit("error", err);
          }
        });

        ws.on("close", () => {
          if (fp && this._peers.has(fp)) {
            this._peers.delete(fp);
            this.deregisterPeerRoutes(fp);
            this._notifyPeerLeft(fp);
          }
        });
      } catch (err) {
        this.emit("error", err);
        ws.close();
      }
    });
  }

  private _authenticate(ws: WebSocket, raw: Buffer): string | null {
    const [header, payload] = decodeFrame(raw);
    if (header.msgType !== MessageType.AUTH) {
      ws.close();
      return null;
    }

    const authData = JSON.parse(payload.toString("utf-8"));
    const peerSecret = authData.secret || "";
    const peerFp = authData.fingerprint || "";

    if (peerSecret !== this.presharedSecret) {
      const fail = encodeFrame(
        MessageType.AUTH_FAIL,
        Buffer.from(JSON.stringify({ error: "bad secret" }), "utf-8"),
      );
      ws.send(fail);
      ws.close();
      return null;
    }

    // Pin the peer fingerprint
    this._pinnedPeers.set(peerFp, true);

    // Send AUTH_OK with our cert info
    const okPayload = Buffer.from(JSON.stringify({
      cert_pem: this.certBundle!.certPem,
      fingerprint: this.certBundle!.fingerprint,
    }), "utf-8");
    const okFrame = encodeFrame(MessageType.AUTH_OK, okPayload);
    ws.send(okFrame);

    // Register tunnel routes if services advertised
    const peerServices = authData.services || [];
    if (peerServices.length > 0) {
      this.registerPeerRoutes(peerFp, peerServices);
    }

    return peerFp;
  }

  private _dispatch(peerFp: string, raw: Buffer): void {
    const [header, payload] = decodeFrame(raw);

    // Stream reassembly
    if (header.flags & (Flags.STREAM_START | Flags.STREAM_CHUNK | Flags.STREAM_END)) {
      this._handleStreamChunk(peerFp, header, payload);
      return;
    }

    // HTTP_RESPONSE — resolve pending tunnel request
    if (header.msgType === MessageType.HTTP_RESPONSE) {
      const midKey = header.msgId.toString("hex");
      const pending = this._pendingRequests.get(midKey);
      if (pending) {
        pending.resolve(payload);
      }
      return;
    }

    // Relay
    if (header.msgType === MessageType.RELAY) {
      this._handleRelay(peerFp, payload);
      return;
    }

    // Regular messages
    this._dispatchToHandler(peerFp, header.msgType, payload);
  }

  private _dispatchToHandler(peerFp: string, msgType: MessageType, payload: Buffer): void {
    const handler = this._handlers.get(msgType);
    if (!handler) return;

    if (msgType === MessageType.JSON) {
      const data = JSON.parse(payload.toString("utf-8"));
      handler(peerFp, data);
    } else if (msgType === MessageType.FILE || msgType === MessageType.IMAGE) {
      const [filename, fileData] = decodeFilePayload(payload);
      handler(peerFp, filename, fileData);
    } else if (msgType === MessageType.BINARY) {
      handler(peerFp, payload);
    } else {
      handler(peerFp, payload);
    }
  }

  private _handleStreamChunk(peerFp: string, header: FrameHeader, payload: Buffer): void {
    const midKey = header.msgId.toString("hex");

    if (header.flags & Flags.STREAM_START) {
      this._streamBuffers.set(midKey, [payload]);
      this._streamMeta.set(midKey, header.msgType);
    } else if (header.flags & Flags.STREAM_CHUNK) {
      const buf = this._streamBuffers.get(midKey);
      if (buf) buf.push(payload);
    }

    if (header.flags & Flags.STREAM_END) {
      const buf = this._streamBuffers.get(midKey);
      if (buf) {
        buf.push(payload);
        const full = Buffer.concat(buf);
        const msgType = this._streamMeta.get(midKey) ?? header.msgType;
        this._streamBuffers.delete(midKey);
        this._streamMeta.delete(midKey);

        if (msgType === MessageType.RELAY) {
          this._handleRelay(peerFp, full);
        } else if (msgType === MessageType.HTTP_RESPONSE) {
          const pending = this._pendingRequests.get(midKey);
          if (pending) pending.resolve(full);
        } else {
          this._dispatchToHandler(peerFp, msgType, full);
        }
      }
    }
  }

  private _handleRelay(senderFp: string, payload: Buffer): void {
    const [, destFp, innerType, innerPayload] = decodeRelayPayload(payload);

    const ws = this._peers.get(destFp);
    if (!ws) return;

    // Re-wrap with actual sender fingerprint
    const relayOut = encodeRelayPayload(senderFp, destFp, innerType, innerPayload);
    this._sendPayloadStreamed(ws, MessageType.RELAY, relayOut);
  }

  private _notifyPeerJoined(newFp: string): void {
    const existingFps = Array.from(this._peers.keys()).filter((fp) => fp !== newFp);

    // Tell new peer about existing peers
    const newWs = this._peers.get(newFp);
    if (newWs && existingFps.length > 0) {
      for (const fp of existingFps) {
        const event = Buffer.from(
          JSON.stringify({ _wire_peer_event: "joined", peer_fp: fp }),
          "utf-8",
        );
        const frame = encodeFrame(MessageType.JSON, event, undefined, Flags.NONE, true);
        newWs.send(frame);
      }
    }

    // Tell existing peers about the new peer
    const event = Buffer.from(
      JSON.stringify({ _wire_peer_event: "joined", peer_fp: newFp }),
      "utf-8",
    );
    const frame = encodeFrame(MessageType.JSON, event, undefined, Flags.NONE, true);
    for (const [fp, ws] of this._peers) {
      if (fp !== newFp) {
        try { ws.send(frame); } catch {}
      }
    }
  }

  private _notifyPeerLeft(goneFp: string): void {
    const event = Buffer.from(
      JSON.stringify({ _wire_peer_event: "left", peer_fp: goneFp }),
      "utf-8",
    );
    const frame = encodeFrame(MessageType.JSON, event, undefined, Flags.NONE, true);
    for (const ws of this._peers.values()) {
      try { ws.send(frame); } catch {}
    }
  }

  private _sendPayloadStreamed(ws: WebSocket, msgType: MessageType, payload: Buffer): void {
    const msgId = Buffer.from(randomUUID().replace(/-/g, ""), "hex");

    if (payload.length <= STREAM_CHUNK_SIZE) {
      const frame = encodeFrame(msgType, payload, msgId, Flags.NONE, true);
      ws.send(frame);
      return;
    }

    let offset = 0;
    let first = true;
    while (offset < payload.length) {
      const chunk = payload.subarray(offset, offset + STREAM_CHUNK_SIZE);
      const isLast = offset + STREAM_CHUNK_SIZE >= payload.length;

      let flags: number;
      if (first) { flags = Flags.STREAM_START; first = false; }
      else if (isLast) { flags = Flags.STREAM_END; }
      else { flags = Flags.STREAM_CHUNK; }

      const frame = encodeFrame(msgType, chunk, msgId, flags);
      ws.send(frame);
      offset += STREAM_CHUNK_SIZE;
    }
  }
}
