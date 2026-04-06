/**
 * ReverseProxy — lightweight HTTP reverse proxy for Node.js.
 *
 * Maps URL path prefixes to upstream HTTP services or WebSocket tunnel routes.
 * Mirrors the Python/Rust proxy modules.
 */

import * as http from "http";
import * as https from "https";

const HOP_BY_HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailers", "transfer-encoding", "upgrade",
]);

export type TunnelFn = (
  peerFp: string, method: string, path: string, query: string,
  headers: [string, string][], body: Buffer, timeout?: number,
) => Promise<[number, [string, string][], Buffer]>;

export interface TunnelRouteInfo {
  peer_fp: string;
  tunnel_fn: TunnelFn;
}

export class ReverseProxy {
  readonly host: string;
  readonly port: number;
  readonly readTimeout: number;

  private _routes: Map<string, string> = new Map();
  private _tunnelRoutes: Map<string, TunnelRouteInfo> = new Map();
  private _server: http.Server | null = null;

  constructor(host: string = "0.0.0.0", port: number = 8080, readTimeout: number = 30) {
    this.host = host;
    this.port = port;
    this.readTimeout = readTimeout;
  }

  addRoute(pathPrefix: string, upstreamUrl: string): void {
    const prefix = "/" + pathPrefix.replace(/^\/|\/$/g, "");
    const upstream = upstreamUrl.replace(/\/$/, "");
    this._routes.set(prefix, upstream);
  }

  addTunnelRoute(pathPrefix: string, peerFp: string, tunnelFn: TunnelFn): void {
    const prefix = "/" + pathPrefix.replace(/^\/|\/$/g, "");
    this._tunnelRoutes.set(prefix, { peer_fp: peerFp, tunnel_fn: tunnelFn });
  }

  removeRoute(pathPrefix: string): void {
    const prefix = "/" + pathPrefix.replace(/^\/|\/$/g, "");
    this._routes.delete(prefix);
    this._tunnelRoutes.delete(prefix);
  }

  get routes(): Map<string, string> {
    return new Map(this._routes);
  }

  matchRoute(path: string): [string | null, string | null, TunnelRouteInfo | null] {
    let bestPrefix: string | null = null;
    let isTunnel = false;

    for (const prefix of this._routes.keys()) {
      if (path === prefix || path.startsWith(prefix + "/") || prefix === "/") {
        if (bestPrefix === null || prefix.length > bestPrefix.length) {
          bestPrefix = prefix;
          isTunnel = false;
        }
      }
    }

    for (const prefix of this._tunnelRoutes.keys()) {
      if (path === prefix || path.startsWith(prefix + "/") || prefix === "/") {
        if (bestPrefix === null || prefix.length > bestPrefix.length) {
          bestPrefix = prefix;
          isTunnel = true;
        }
      }
    }

    if (bestPrefix === null) return [null, null, null];

    let remainder = bestPrefix === "/" ? path : path.substring(bestPrefix.length);
    if (!remainder.startsWith("/")) remainder = "/" + remainder;

    if (isTunnel) {
      return [null, remainder, this._tunnelRoutes.get(bestPrefix)!];
    }
    return [this._routes.get(bestPrefix)!, remainder, null];
  }

  async start(): Promise<void> {
    this._server = http.createServer((req, res) => {
      this._handle(req, res).catch((err) => {
        res.writeHead(502);
        res.end("Bad Gateway");
      });
    });

    await new Promise<void>((resolve) => {
      this._server!.listen(this.port, this.host, () => resolve());
    });
  }

  async stop(): Promise<void> {
    if (this._server) {
      await new Promise<void>((resolve) => {
        this._server!.close(() => resolve());
      });
      this._server = null;
    }
  }

  // ── Internal ──────────────────────────────────────────────────────────────

  private async _handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const urlObj = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    const path = urlObj.pathname;
    const queryString = urlObj.search ? urlObj.search.substring(1) : "";

    const [upstream, remainder, tunnelInfo] = this.matchRoute(path);

    if (upstream === null && tunnelInfo === null) {
      res.writeHead(404);
      res.end("No matching route");
      return;
    }

    // Read body
    const bodyChunks: Buffer[] = [];
    for await (const chunk of req) {
      bodyChunks.push(Buffer.from(chunk));
    }
    const body = Buffer.concat(bodyChunks);

    // Tunnel route
    if (tunnelInfo !== null) {
      const headers = this._forwardRequestHeaders(req);
      const peer = req.socket.remoteAddress || "unknown";
      headers.push(["X-Forwarded-For", peer]);
      headers.push(["X-Forwarded-Host", req.headers.host || ""]);
      headers.push(["X-Forwarded-Proto", "http"]);

      const [status, respHeaders, respBody] = await tunnelInfo.tunnel_fn(
        tunnelInfo.peer_fp,
        req.method || "GET",
        path,
        queryString,
        headers,
        body,
        this.readTimeout * 1000,
      );

      const headerObj: Record<string, string> = {};
      for (const [k, v] of respHeaders) headerObj[k] = v;
      res.writeHead(status, headerObj);
      res.end(respBody);
      return;
    }

    // Direct route
    let targetUrl = upstream! + remainder!;
    if (queryString) targetUrl += "?" + queryString;

    const fwdHeaders = this._forwardRequestHeaders(req);
    const peer = req.socket.remoteAddress || "unknown";
    fwdHeaders.push(["X-Forwarded-For", peer]);
    fwdHeaders.push(["X-Forwarded-Host", req.headers.host || ""]);
    fwdHeaders.push(["X-Forwarded-Proto", "http"]);

    const headerObj: Record<string, string> = {};
    for (const [k, v] of fwdHeaders) headerObj[k] = v;

    try {
      const parsedUrl = new URL(targetUrl);
      const client = parsedUrl.protocol === "https:" ? https : http;
      const proxyReq = client.request(targetUrl, {
        method: req.method,
        headers: headerObj,
      }, (proxyRes) => {
        const respHeaders = this._forwardResponseHeaders(proxyRes);
        res.writeHead(proxyRes.statusCode || 502, respHeaders);
        proxyRes.pipe(res);
      });

      proxyReq.on("error", () => {
        res.writeHead(502);
        res.end("Bad Gateway");
      });

      if (body.length > 0) proxyReq.write(body);
      proxyReq.end();
    } catch {
      res.writeHead(502);
      res.end("Bad Gateway");
    }
  }

  private _forwardRequestHeaders(req: http.IncomingMessage): [string, string][] {
    const headers: [string, string][] = [];
    for (const [key, val] of Object.entries(req.headers)) {
      if (val && !HOP_BY_HOP.has(key.toLowerCase()) && key.toLowerCase() !== "host") {
        headers.push([key, Array.isArray(val) ? val.join(", ") : val]);
      }
    }
    return headers;
  }

  private _forwardResponseHeaders(proxyRes: http.IncomingMessage): Record<string, string> {
    const headers: Record<string, string> = {};
    for (const [key, val] of Object.entries(proxyRes.headers)) {
      if (val && !HOP_BY_HOP.has(key.toLowerCase())) {
        headers[key] = Array.isArray(val) ? val.join(", ") : val;
      }
    }
    return headers;
  }
}
