import { describe, it, afterEach } from "node:test";
import * as assert from "node:assert/strict";
import * as http from "http";
import { ReverseProxy } from "../src/proxy";

let _port = 27000;
function nextPort(): number { return _port++; }

let cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups) await fn();
  cleanups = [];
});

// Simple echo upstream
function createEchoServer(port: number): Promise<http.Server> {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks).toString("utf-8");

      const result = JSON.stringify({
        method: req.method,
        path: req.url,
        headers: req.headers,
        body: body || undefined,
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(result);
    });
    server.listen(port, "127.0.0.1", () => resolve(server));
  });
}

function fetch(url: string, options: { method?: string; headers?: Record<string, string>; body?: string } = {}): Promise<{ status: number; body: string; headers: http.IncomingHttpHeaders }> {
  return new Promise((resolve, reject) => {
    const req = http.request(url, { method: options.method || "GET", headers: options.headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on("data", (c: Buffer) => chunks.push(c));
      res.on("end", () => resolve({
        status: res.statusCode!,
        body: Buffer.concat(chunks).toString("utf-8"),
        headers: res.headers,
      }));
    });
    req.on("error", reject);
    if (options.body) req.write(options.body);
    req.end();
  });
}

describe("Route matching", () => {
  it("exact prefix", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/api", "http://backend:3000");
    const [upstream, remainder] = proxy.matchRoute("/api/users");
    assert.equal(upstream, "http://backend:3000");
    assert.equal(remainder, "/users");
  });

  it("subpath", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/api", "http://backend:3000");
    const [upstream, remainder] = proxy.matchRoute("/api/v2/items/42");
    assert.equal(upstream, "http://backend:3000");
    assert.equal(remainder, "/v2/items/42");
  });

  it("no match", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/api", "http://backend:3000");
    const [upstream, , tunnel] = proxy.matchRoute("/other");
    assert.equal(upstream, null);
    assert.equal(tunnel, null);
  });

  it("longest prefix wins", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/api", "http://backend-a:3000");
    proxy.addRoute("/api/v2", "http://backend-b:4000");
    const [upstream, remainder] = proxy.matchRoute("/api/v2/items");
    assert.equal(upstream, "http://backend-b:4000");
    assert.equal(remainder, "/items");
  });

  it("root route catches all", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/", "http://fallback:5000");
    const [upstream, remainder] = proxy.matchRoute("/anything/here");
    assert.equal(upstream, "http://fallback:5000");
    assert.equal(remainder, "/anything/here");
  });

  it("add and remove route", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/api", "http://backend:3000");
    assert.equal(proxy.routes.size, 1);
    proxy.removeRoute("/api");
    assert.equal(proxy.routes.size, 0);
    const [upstream] = proxy.matchRoute("/api/test");
    assert.equal(upstream, null);
  });

  it("trailing slash normalised", () => {
    const proxy = new ReverseProxy();
    proxy.addRoute("/api/", "http://backend:3000/");
    const [upstream] = proxy.matchRoute("/api/test");
    assert.equal(upstream, "http://backend:3000");
  });
});

describe("HTTP forwarding", () => {
  it("GET forwarded", async () => {
    const upPort = nextPort();
    const proxyPort = nextPort();

    const upstream = await createEchoServer(upPort);
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", `http://127.0.0.1:${upPort}`);
    await proxy.start();

    cleanups.push(async () => {
      await proxy.stop();
      upstream.close();
    });

    const resp = await fetch(`http://127.0.0.1:${proxyPort}/api/users`);
    assert.equal(resp.status, 200);
    const data = JSON.parse(resp.body);
    assert.equal(data.method, "GET");
    assert.equal(data.path, "/users");
  });

  it("POST with body", async () => {
    const upPort = nextPort();
    const proxyPort = nextPort();

    const upstream = await createEchoServer(upPort);
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", `http://127.0.0.1:${upPort}`);
    await proxy.start();

    cleanups.push(async () => {
      await proxy.stop();
      upstream.close();
    });

    const resp = await fetch(`http://127.0.0.1:${proxyPort}/api/items`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: '{"name":"test"}',
    });
    assert.equal(resp.status, 200);
    const data = JSON.parse(resp.body);
    assert.equal(data.method, "POST");
    assert.equal(data.body, '{"name":"test"}');
  });

  it("query string forwarded", async () => {
    const upPort = nextPort();
    const proxyPort = nextPort();

    const upstream = await createEchoServer(upPort);
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", `http://127.0.0.1:${upPort}`);
    await proxy.start();

    cleanups.push(async () => {
      await proxy.stop();
      upstream.close();
    });

    const resp = await fetch(`http://127.0.0.1:${proxyPort}/api/search?q=hello&page=2`);
    const data = JSON.parse(resp.body);
    assert.ok(data.path.includes("q=hello"));
  });

  it("X-Forwarded headers", async () => {
    const upPort = nextPort();
    const proxyPort = nextPort();

    const upstream = await createEchoServer(upPort);
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", `http://127.0.0.1:${upPort}`);
    await proxy.start();

    cleanups.push(async () => {
      await proxy.stop();
      upstream.close();
    });

    const resp = await fetch(`http://127.0.0.1:${proxyPort}/api/test`);
    const data = JSON.parse(resp.body);
    assert.ok(data.headers["x-forwarded-for"]);
    assert.ok(data.headers["x-forwarded-host"]);
    assert.ok(data.headers["x-forwarded-proto"]);
  });

  it("404 no matching route", async () => {
    const proxyPort = nextPort();
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", "http://127.0.0.1:99999");
    await proxy.start();

    cleanups.push(async () => { await proxy.stop(); });

    const resp = await fetch(`http://127.0.0.1:${proxyPort}/other/path`);
    assert.equal(resp.status, 404);
  });

  it("502 unreachable upstream", async () => {
    const proxyPort = nextPort();
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", "http://127.0.0.1:1"); // Port 1 = unreachable
    await proxy.start();

    cleanups.push(async () => { await proxy.stop(); });

    const resp = await fetch(`http://127.0.0.1:${proxyPort}/api/test`);
    assert.equal(resp.status, 502);
  });

  it("multiple routes", async () => {
    const upPort = nextPort();
    const proxyPort = nextPort();

    const upstream = await createEchoServer(upPort);
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", `http://127.0.0.1:${upPort}`);
    proxy.addRoute("/admin", `http://127.0.0.1:${upPort}`);
    await proxy.start();

    cleanups.push(async () => {
      await proxy.stop();
      upstream.close();
    });

    const r1 = await fetch(`http://127.0.0.1:${proxyPort}/api/users`);
    const d1 = JSON.parse(r1.body);
    assert.equal(d1.path, "/users");

    const r2 = await fetch(`http://127.0.0.1:${proxyPort}/admin/dashboard`);
    const d2 = JSON.parse(r2.body);
    assert.equal(d2.path, "/dashboard");
  });

  it("PUT and DELETE methods", async () => {
    const upPort = nextPort();
    const proxyPort = nextPort();

    const upstream = await createEchoServer(upPort);
    const proxy = new ReverseProxy("127.0.0.1", proxyPort);
    proxy.addRoute("/api", `http://127.0.0.1:${upPort}`);
    await proxy.start();

    cleanups.push(async () => {
      await proxy.stop();
      upstream.close();
    });

    const r1 = await fetch(`http://127.0.0.1:${proxyPort}/api/items/1`, { method: "PUT", body: '{"updated":true}' });
    assert.equal(JSON.parse(r1.body).method, "PUT");

    const r2 = await fetch(`http://127.0.0.1:${proxyPort}/api/items/1`, { method: "DELETE" });
    assert.equal(JSON.parse(r2.body).method, "DELETE");
  });
});
