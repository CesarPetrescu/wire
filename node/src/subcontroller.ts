/**
 * SubController (client) — connects to a Controller over WSS.
 *
 * Mirrors the Python SubController's capabilities:
 *   - Generates its own self-signed cert on startup
 *   - Authenticates with pre-shared secret
 *   - Pins the Controller's cert fingerprint
 *   - Full bidirectional JSON / Binary / File / Image / Stream
 *   - Peer-to-peer relay via Controller
 *   - HTTP tunnel request handling
 *   - Automatic reconnection with exponential backoff
 */

import { EventEmitter } from "events";
import * as http from "http";
import * as https from "https";
import WebSocket from "ws";
import { CertBundle, generateSelfSignedCert, createClientTlsOptions } from "./certs";
import {
  STREAM_CHUNK_SIZE,
  MessageType,
  Flags,
  HttpMethod,
  httpMethodFromStr,
  httpMethodToStr,
  encodeFrame,
  decodeFrame,
  encodeFilePayload,
  decodeFilePayload,
  encodeRelayPayload,
  decodeRelayPayload,
  encodeHttpRequest,
  decodeHttpRequest,
  encodeHttpResponse,
  decodeHttpResponse,
  FrameHeader,
} from "./protocol";
import { randomUUID } from "crypto";

export interface ServiceDef {
  prefix: string;
  upstream: string;
  health_check?: string;
}

export interface SubControllerOptions {
  controllerUrl: string;
  presharedSecret: string;
  services?: ServiceDef[];
}

export class SubController extends EventEmitter {
  readonly controllerUrl: string;
  readonly presharedSecret: string;
  readonly services: ServiceDef[];

  certBundle: CertBundle | null = null;
  controllerFingerprint: string | null = null;

  private _ws: WebSocket | null = null;
  private _knownPeers: Set<string> = new Set();

  // Stream reassembly
  private _streamBuffers: Map<string, Buffer[]> = new Map();
  private _streamMeta: Map<string, MessageType> = new Map();

  // Reconnection config
  private _reconnectEnabled = false;
  private _reconnectInitialDelay = 1.0;
  private _reconnectMaxDelay = 30.0;
  private _reconnectMaxAttempts = 0;
  private _disconnecting = false;

  constructor(options: SubControllerOptions) {
    super();
    this.controllerUrl = options.controllerUrl;
    this.presharedSecret = options.presharedSecret;
    this.services = options.services ?? [];
  }

  get fingerprint(): string | null {
    return this.certBundle?.fingerprint ?? null;
  }

  get knownPeers(): string[] {
    return Array.from(this._knownPeers);
  }

  configureReconnect(options: {
    enabled?: boolean;
    initialDelay?: number;
    maxDelay?: number;
    maxAttempts?: number;
  }): void {
    if (options.enabled !== undefined) this._reconnectEnabled = options.enabled;
    if (options.initialDelay !== undefined) this._reconnectInitialDelay = options.initialDelay;
    if (options.maxDelay !== undefined) this._reconnectMaxDelay = options.maxDelay;
    if (options.maxAttempts !== undefined) this._reconnectMaxAttempts = options.maxAttempts;
  }

  // ── Connection lifecycle ──────────────────────────────────────────────────

  async connect(): Promise<void> {
    this._disconnecting = false;
    this.certBundle = generateSelfSignedCert("subcontroller");

    const tlsOpts = createClientTlsOptions(this.certBundle);

    await new Promise<void>((resolve, reject) => {
      this._ws = new WebSocket(this.controllerUrl, { ...tlsOpts });

      this._ws.once("open", async () => {
        try {
          await this._authenticate();
          this._setupListeners();
          resolve();
        } catch (err) {
          reject(err);
        }
      });

      this._ws.once("error", (err: Error) => {
        reject(err);
      });
    });
  }

  async disconnect(): Promise<void> {
    this._disconnecting = true;
    if (this._ws) {
      this._ws.removeAllListeners();
      this._ws.close();
      this._ws = null;
    }
  }

  // ── Send methods ──────────────────────────────────────────────────────────

  async sendJson(data: any): Promise<void> {
    this._ensureConnected();
    const payload = Buffer.from(JSON.stringify(data), "utf-8");
    await this._sendPayloadStreamed(MessageType.JSON, payload);
  }

  async sendBinary(data: Buffer): Promise<void> {
    this._ensureConnected();
    await this._sendPayloadStreamed(MessageType.BINARY, data);
  }

  async sendFile(filename: string, data: Buffer, isImage = false): Promise<void> {
    this._ensureConnected();
    const msgType = isImage ? MessageType.IMAGE : MessageType.FILE;
    const filePayload = encodeFilePayload(filename, data);
    await this._sendPayloadStreamed(msgType, filePayload);
  }

  // ── Peer-to-peer via relay ────────────────────────────────────────────────

  async sendJsonToPeer(destFp: string, data: any): Promise<void> {
    this._ensureConnected();
    const inner = Buffer.from(JSON.stringify(data), "utf-8");
    const relay = encodeRelayPayload(this.certBundle!.fingerprint, destFp, MessageType.JSON, inner);
    await this._sendPayloadStreamed(MessageType.RELAY, relay);
  }

  async sendBinaryToPeer(destFp: string, data: Buffer): Promise<void> {
    this._ensureConnected();
    const relay = encodeRelayPayload(this.certBundle!.fingerprint, destFp, MessageType.BINARY, data);
    await this._sendPayloadStreamed(MessageType.RELAY, relay);
  }

  async sendFileToPeer(
    destFp: string, filename: string, data: Buffer, isImage = false,
  ): Promise<void> {
    this._ensureConnected();
    const innerType = isImage ? MessageType.IMAGE : MessageType.FILE;
    const inner = encodeFilePayload(filename, data);
    const relay = encodeRelayPayload(this.certBundle!.fingerprint, destFp, innerType, inner);
    await this._sendPayloadStreamed(MessageType.RELAY, relay);
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private _ensureConnected(): void {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) {
      throw new Error("Not connected. Call connect() first.");
    }
  }

  private async _authenticate(): Promise<void> {
    const authData: any = {
      secret: this.presharedSecret,
      cert_pem: this.certBundle!.certPem,
      fingerprint: this.certBundle!.fingerprint,
    };
    if (this.services.length > 0) {
      authData.services = this.services;
    }

    const payload = Buffer.from(JSON.stringify(authData), "utf-8");
    const frame = encodeFrame(MessageType.AUTH, payload);
    this._ws!.send(frame);

    // Wait for AUTH_OK or AUTH_FAIL
    const response = await this._waitForMessage(10000);
    const [header, respPayload] = decodeFrame(Buffer.from(response as Buffer));

    if (header.msgType === MessageType.AUTH_FAIL) {
      const err = JSON.parse(respPayload.toString("utf-8"));
      throw new Error(`Authentication failed: ${err.error || "unknown"}`);
    }
    if (header.msgType !== MessageType.AUTH_OK) {
      throw new Error(`Unexpected auth response: ${header.msgType}`);
    }

    const okData = JSON.parse(respPayload.toString("utf-8"));
    const ctrlFp = okData.fingerprint;

    if (this.controllerFingerprint === null) {
      this.controllerFingerprint = ctrlFp;
    } else if (this.controllerFingerprint !== ctrlFp) {
      throw new Error(
        `Controller fingerprint mismatch! Expected ${this.controllerFingerprint.substring(0, 16)}..., got ${ctrlFp.substring(0, 16)}...`,
      );
    }
  }

  private _waitForMessage(timeoutMs: number): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error("Auth timeout"));
      }, timeoutMs);

      const handler = (data: Buffer) => {
        clearTimeout(timer);
        resolve(Buffer.from(data));
      };

      this._ws!.once("message", handler);
    });
  }

  private _setupListeners(): void {
    this._ws!.on("message", (data: Buffer) => {
      try {
        this._dispatch(Buffer.from(data));
      } catch (err) {
        this.emit("error", err);
      }
    });

    this._ws!.on("close", () => {
      if (!this._disconnecting && this._reconnectEnabled) {
        this._reconnectLoop();
      }
    });

    this._ws!.on("error", (err: Error) => {
      this.emit("error", err);
    });
  }

  private _dispatch(raw: Buffer): void {
    const [header, payload] = decodeFrame(raw);

    // Stream reassembly
    if (header.flags & (Flags.STREAM_START | Flags.STREAM_CHUNK | Flags.STREAM_END)) {
      this._handleStreamChunk(header, payload);
      return;
    }

    // HTTP_REQUEST from controller
    if (header.msgType === MessageType.HTTP_REQUEST) {
      this._handleHttpRequest(header.msgId, payload);
      return;
    }

    // Relay from another SubController
    if (header.msgType === MessageType.RELAY) {
      this._dispatchRelay(payload);
      return;
    }

    // JSON (intercept peer events)
    if (header.msgType === MessageType.JSON) {
      const data = JSON.parse(payload.toString("utf-8"));
      if (data._wire_peer_event) {
        this._handlePeerEvent(data);
        return;
      }
      this.emit("json", data);
      return;
    }

    if (header.msgType === MessageType.BINARY) {
      this.emit("binary", payload);
    } else if (header.msgType === MessageType.FILE) {
      const [filename, fileData] = decodeFilePayload(payload);
      this.emit("file", filename, fileData);
    } else if (header.msgType === MessageType.IMAGE) {
      const [filename, fileData] = decodeFilePayload(payload);
      this.emit("image", filename, fileData);
    }
  }

  private _handleStreamChunk(header: FrameHeader, payload: Buffer): void {
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
          this._dispatchRelay(full);
        } else if (msgType === MessageType.JSON) {
          const data = JSON.parse(full.toString("utf-8"));
          if (data._wire_peer_event) {
            this._handlePeerEvent(data);
          } else {
            this.emit("json", data);
          }
        } else if (msgType === MessageType.FILE) {
          const [fn, fd] = decodeFilePayload(full);
          this.emit("file", fn, fd);
        } else if (msgType === MessageType.IMAGE) {
          const [fn, fd] = decodeFilePayload(full);
          this.emit("image", fn, fd);
        } else if (msgType === MessageType.BINARY) {
          this.emit("binary", full);
        }
      }
    }
  }

  private _dispatchRelay(payload: Buffer): void {
    const [sourceFp, , innerType, innerPayload] = decodeRelayPayload(payload);

    if (innerType === MessageType.JSON) {
      const data = JSON.parse(innerPayload.toString("utf-8"));
      this.emit("relay:json", sourceFp, data);
    } else if (innerType === MessageType.BINARY) {
      this.emit("relay:binary", sourceFp, innerPayload);
    } else if (innerType === MessageType.FILE) {
      const [fn, fd] = decodeFilePayload(innerPayload);
      this.emit("relay:file", sourceFp, fn, fd);
    } else if (innerType === MessageType.IMAGE) {
      const [fn, fd] = decodeFilePayload(innerPayload);
      this.emit("relay:image", sourceFp, fn, fd);
    }
  }

  private _handlePeerEvent(data: any): void {
    const event = data._wire_peer_event;
    const peerFp = data.peer_fp;
    if (event === "joined") {
      this._knownPeers.add(peerFp);
      this.emit("peer:joined", peerFp);
    } else if (event === "left") {
      this._knownPeers.delete(peerFp);
      this.emit("peer:left", peerFp);
    }
  }

  private async _handleHttpRequest(msgId: Buffer, payload: Buffer): Promise<void> {
    const [method, path, query, headers, body] = decodeHttpRequest(payload);

    // Find matching service
    const match = this._matchService(path);
    let respPayload: Buffer;

    if (!match) {
      respPayload = encodeHttpResponse(404, [], Buffer.from("No matching service"));
    } else {
      const [upstream, remainder] = match;
      let targetUrl = upstream + remainder;
      if (query) targetUrl += "?" + query;

      try {
        const result = await this._forwardToUpstream(
          httpMethodToStr(method), targetUrl, headers, body,
        );
        respPayload = encodeHttpResponse(result.status, result.headers, result.body);
      } catch (err) {
        respPayload = encodeHttpResponse(502, [], Buffer.from("Bad Gateway"));
      }
    }

    const frame = encodeFrame(MessageType.HTTP_RESPONSE, respPayload, msgId, Flags.NONE, true);
    if (this._ws && this._ws.readyState === WebSocket.OPEN) {
      this._ws.send(frame);
    }
  }

  private _matchService(path: string): [string, string] | null {
    let bestPrefix: string | null = null;
    let bestUpstream: string | null = null;

    for (const svc of this.services) {
      const prefix = "/" + svc.prefix.replace(/^\/|\/$/g, "");
      const matches =
        prefix === "/" || path === prefix || path.startsWith(prefix + "/");
      if (matches && (bestPrefix === null || prefix.length > bestPrefix.length)) {
        bestPrefix = prefix;
        bestUpstream = svc.upstream.replace(/\/$/, "");
      }
    }

    if (!bestPrefix || !bestUpstream) return null;

    let remainder = bestPrefix === "/" ? path : path.substring(bestPrefix.length);
    if (!remainder.startsWith("/")) remainder = "/" + remainder;
    return [bestUpstream, remainder];
  }

  private _forwardToUpstream(
    method: string,
    url: string,
    headers: [string, string][],
    body: Buffer,
  ): Promise<{ status: number; headers: [string, string][]; body: Buffer }> {
    return new Promise((resolve, reject) => {
      const parsedUrl = new URL(url);
      const client = parsedUrl.protocol === "https:" ? https : http;

      const reqHeaders: Record<string, string> = {};
      for (const [k, v] of headers) {
        const lower = k.toLowerCase();
        if (lower !== "host" && lower !== "connection" && lower !== "transfer-encoding") {
          reqHeaders[k] = v;
        }
      }

      const req = client.request(
        url,
        { method, headers: reqHeaders },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk: Buffer) => chunks.push(chunk));
          res.on("end", () => {
            const respHeaders: [string, string][] = [];
            for (const [key, val] of Object.entries(res.headers)) {
              if (val && key.toLowerCase() !== "transfer-encoding" && key.toLowerCase() !== "connection") {
                respHeaders.push([key, Array.isArray(val) ? val.join(", ") : val]);
              }
            }
            resolve({
              status: res.statusCode ?? 502,
              headers: respHeaders,
              body: Buffer.concat(chunks),
            });
          });
          res.on("error", reject);
        },
      );

      req.on("error", reject);
      if (body.length > 0) req.write(body);
      req.end();
    });
  }

  private async _sendPayloadStreamed(
    msgType: MessageType,
    payload: Buffer,
  ): Promise<void> {
    const msgId = Buffer.from(randomUUID().replace(/-/g, ""), "hex");

    if (payload.length <= STREAM_CHUNK_SIZE) {
      const frame = encodeFrame(msgType, payload, msgId, Flags.NONE, true);
      this._ws!.send(frame);
      return;
    }

    let offset = 0;
    let first = true;
    while (offset < payload.length) {
      const chunk = payload.subarray(offset, offset + STREAM_CHUNK_SIZE);
      const isLast = offset + STREAM_CHUNK_SIZE >= payload.length;

      let flags: number;
      if (first) {
        flags = Flags.STREAM_START;
        first = false;
      } else if (isLast) {
        flags = Flags.STREAM_END;
      } else {
        flags = Flags.STREAM_CHUNK;
      }

      const frame = encodeFrame(msgType, chunk, msgId, flags);
      this._ws!.send(frame);
      offset += STREAM_CHUNK_SIZE;
    }
  }

  private async _reconnectLoop(): Promise<void> {
    let delay = this._reconnectInitialDelay;
    let attempts = 0;

    while (!this._disconnecting) {
      if (this._reconnectMaxAttempts > 0 && attempts >= this._reconnectMaxAttempts) {
        break;
      }
      attempts++;

      await new Promise((r) => setTimeout(r, delay * 1000));

      try {
        const tlsOpts = createClientTlsOptions(this.certBundle!);
        await new Promise<void>((resolve, reject) => {
          this._ws = new WebSocket(this.controllerUrl, { ...tlsOpts });
          this._ws.once("open", async () => {
            try {
              await this._authenticate();
              this._setupListeners();
              resolve();
            } catch (err) {
              reject(err);
            }
          });
          this._ws.once("error", reject);
        });
        // Successfully reconnected
        delay = this._reconnectInitialDelay;
        return;
      } catch {
        delay = Math.min(delay * 2, this._reconnectMaxDelay);
      }
    }
  }
}
