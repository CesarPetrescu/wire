import { describe, it, afterEach } from "node:test";
import * as assert from "node:assert/strict";
import * as http from "http";
import { Controller } from "../src/controller";
import { SubController } from "../src/subcontroller";
import { MessageType } from "../src/protocol";

const SECRET = "tunnel-test-secret-77";
let _port = 28000;
function nextPort(): number { return _port++; }

let cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  for (const fn of cleanups.reverse()) {
    try { await fn(); } catch {}
  }
  cleanups = [];
});

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// Echo upstream: returns request info as JSON
function createEchoServer(port: number): Promise<http.Server> {
  return new Promise((resolve) => {
    const server = http.createServer(async (req, res) => {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(Buffer.from(chunk));
      const body = Buffer.concat(chunks).toString("utf-8");
      const result = JSON.stringify({
        method: req.method,
        path: req.url,
        body: body || undefined,
      });
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(result);
    });
    server.listen(port, "127.0.0.1", () => resolve(server));
  });
}

describe("Tunnel route management", () => {
  it("route registration", async () => {
    const ctrlPort = nextPort();
    const upPort = nextPort();

    const ctrl = new Controller("127.0.0.1", ctrlPort, SECRET);
    await ctrl.start();

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${ctrlPort}`,
      presharedSecret: SECRET,
      services: [
        { prefix: "/api", upstream: `http://127.0.0.1:${upPort}` },
        { prefix: "/admin", upstream: `http://127.0.0.1:${upPort}` },
      ],
    });
    await sub.connect();

    cleanups.push(async () => {
      await sub.disconnect();
      await ctrl.stop();
    });

    const routes = ctrl.tunnelRoutes;
    assert.equal(routes.size, 2);
    assert.ok(routes.has("/api"));
    assert.ok(routes.has("/admin"));
    assert.equal(routes.get("/api")!.peer_fp, sub.fingerprint);
  });

  it("route cleanup on disconnect", async () => {
    const ctrlPort = nextPort();
    const upPort = nextPort();

    const ctrl = new Controller("127.0.0.1", ctrlPort, SECRET);
    await ctrl.start();

    cleanups.push(async () => { await ctrl.stop(); });

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${ctrlPort}`,
      presharedSecret: SECRET,
      services: [{ prefix: "/api", upstream: `http://127.0.0.1:${upPort}` }],
    });
    await sub.connect();

    assert.equal(ctrl.tunnelRoutes.size, 1);

    await sub.disconnect();
    await sleep(200);

    assert.equal(ctrl.tunnelRoutes.size, 0);
  });

  it("no services backward compat", async () => {
    const ctrlPort = nextPort();

    const ctrl = new Controller("127.0.0.1", ctrlPort, SECRET);
    await ctrl.start();

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${ctrlPort}`,
      presharedSecret: SECRET,
    });
    await sub.connect();

    cleanups.push(async () => {
      await sub.disconnect();
      await ctrl.stop();
    });

    assert.equal(ctrl.tunnelRoutes.size, 0);
    assert.equal(ctrl.peerFingerprints.length, 1);
  });
});

describe("Tunnel HTTP forwarding", () => {
  it("GET through tunnel", async () => {
    const ctrlPort = nextPort();
    const upPort = nextPort();

    const upstream = await createEchoServer(upPort);
    cleanups.push(async () => { upstream.close(); });

    const ctrl = new Controller("127.0.0.1", ctrlPort, SECRET);
    await ctrl.start();

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${ctrlPort}`,
      presharedSecret: SECRET,
      services: [{ prefix: "/api", upstream: `http://127.0.0.1:${upPort}` }],
    });
    await sub.connect();

    cleanups.push(async () => {
      await sub.disconnect();
      await ctrl.stop();
    });

    const [status, headers, body] = await ctrl.tunnelRequest(
      sub.fingerprint!, "GET", "/api/users", "page=1", [], Buffer.alloc(0),
    );

    assert.equal(status, 200);
    const data = JSON.parse(body.toString("utf-8"));
    assert.equal(data.method, "GET");
    assert.ok(data.path.includes("/users"));
  });

  it("POST with body through tunnel", async () => {
    const ctrlPort = nextPort();
    const upPort = nextPort();

    const upstream = await createEchoServer(upPort);
    cleanups.push(async () => { upstream.close(); });

    const ctrl = new Controller("127.0.0.1", ctrlPort, SECRET);
    await ctrl.start();

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${ctrlPort}`,
      presharedSecret: SECRET,
      services: [{ prefix: "/api", upstream: `http://127.0.0.1:${upPort}` }],
    });
    await sub.connect();

    cleanups.push(async () => {
      await sub.disconnect();
      await ctrl.stop();
    });

    const reqBody = Buffer.from('{"name":"test-item"}');
    const [status, headers, respBody] = await ctrl.tunnelRequest(
      sub.fingerprint!, "POST", "/api/items", "",
      [["Content-Type", "application/json"]], reqBody,
    );

    assert.equal(status, 200);
    const data = JSON.parse(respBody.toString("utf-8"));
    assert.equal(data.method, "POST");
    assert.equal(data.body, '{"name":"test-item"}');
  });

  it("tunnel direct request", async () => {
    const ctrlPort = nextPort();
    const upPort = nextPort();

    const upstream = await createEchoServer(upPort);
    cleanups.push(async () => { upstream.close(); });

    const ctrl = new Controller("127.0.0.1", ctrlPort, SECRET);
    await ctrl.start();

    const sub = new SubController({
      controllerUrl: `wss://127.0.0.1:${ctrlPort}`,
      presharedSecret: SECRET,
      services: [{ prefix: "/svc", upstream: `http://127.0.0.1:${upPort}` }],
    });
    await sub.connect();

    cleanups.push(async () => {
      await sub.disconnect();
      await ctrl.stop();
    });

    // Direct tunnel_request bypassing proxy
    const [status, , body] = await ctrl.tunnelRequest(
      sub.fingerprint!, "GET", "/svc/health", "", [], Buffer.alloc(0),
    );
    assert.equal(status, 200);
  });
});
