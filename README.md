# Wire ⚡

A modern **secure WebSocket framework** with matching **Python** and **Rust** implementations that speak the same binary protocol.

Wire is designed for private environments (LAN, edge, internal automation) where you need:

- ✅ **Controller + SubController topology**
- ✅ **JSON + binary + file + image** message transport
- ✅ **Streaming/chunked transfer** for large payloads
- ✅ **Identity trust with certificate pinning**
- ✅ **Python↔Rust interoperability** in production-like tests

> `Wire` is the foundation for lightweight private channels where reliability and compatibility matter.

---

## Why Wire? 🌐

When you have one orchestration service (Controller) and many workers (SubControllers), Wire gives you:

- **No external CA dependency** (self-signed cert flow)
- **Shared-secret gatekeeping** at connect time
- **Message-type consistency** across languages
- **Simple extensibility** for new handlers and integrations

Perfect for:
- ✅ Home lab control planes
- ✅ Internal tooling buses
- ✅ Sensor/agent networks
- ✅ Private file transfer bridges

---

## Protocol at a glance 🧬

All frames use a fixed **24-byte header** + payload.

| Offset | Size | Field |
|---|---:|---|
| 0 | 2 | Magic (`0xBE01`) |
| 2 | 1 | Message type |
| 3 | 16 | Message ID (`UUID`) |
| 19 | 1 | Flags |
| 20 | 4 | Payload length |
| 24 | ... | Payload bytes |

### Message types 🔌

| Hex | Type | Meaning |
|---|---|---|
| `0x01` | `JSON` | JSON payload |
| `0x02` | `BINARY` | Arbitrary binary |
| `0x03` | `FILE` | Named file chunk |
| `0x04` | `IMAGE` | Named image payload |
| `0x10` | `AUTH` | Authentication handshake |
| `0x11` | `AUTH_OK` | Handshake accepted |
| `0x12` | `AUTH_FAIL` | Handshake rejected |
| `0xFF` | `PING` | Keepalive |

### Flags 🧭

| Bit | Flag | Meaning |
|---|---|---|
| 0 | `STREAM_START` | first chunk |
| 1 | `STREAM_CHUNK` | middle chunk |
| 2 | `STREAM_END` | final chunk |
| 3 | `COMPRESSED` | payload compressed |

Large payloads are streamed automatically once above chunking threshold; checksums ensure integrity where applicable.

---

## Security model 🛡️

Wire uses a practical identity model:

1. Node starts -> generates **ECDSA P-256** cert/key.
2. SubController initiates with `AUTH` containing secret + certificate.
3. Controller validates secret.
4. On success, Controller sends `AUTH_OK` + its cert.
5. Both peers store pinned SHA-256 fingerprints.
6. On reconnect, peer mismatch triggers disconnect.

This gives **identity continuity** without a CA dependency.

---

## Project structure 📁

```text
wire/
  examples/
    reverse-proxy/
      basic_proxy.py / .rs
      multi_service_proxy.py / .rs
      dynamic_routes_proxy.py / .rs
      versioned_api_proxy.py / .rs
      wire_with_proxy.py          # combined controller + proxy

  python/
    wire/
      __init__.py
      protocol.py
      certs.py
      controller.py
      subcontroller.py
      proxy.py                    # ReverseProxy
    tests/
      test_protocol.py
      test_certs.py
      test_integration.py
      test_cross_language.py
      test_star_topology.py
      test_proxy.py
    requirements.txt

  rust/wire-rs/
    src/
      lib.rs
      protocol.rs
      certs.rs
      controller.rs
      subcontroller.rs
      proxy.rs                    # ReverseProxy
      main.rs
    tests/
      lib.rs
      integration.rs
      star_topology.rs
      proxy.rs
    Cargo.toml
```

---

## Quick start 🚀

### 1) Clone + enter repo

```bash
git clone git@github.com:CesarPetrescu/wire.git
cd wire
```

### 2) Python setup (optional for mixed-language testing)

```bash
cd python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Rust setup

```bash
cd rust/wire-rs
cargo build --release
```

### 4) Run a tiny end-to-end check

- Start controller (Python or Rust)
- Start subcontroller (the other language)
- Send JSON and verify response

---

## Python API usage 🐍

### Controller

```python
import asyncio
from wire import Controller, MessageType

async def main():
    ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="my-secret")

    @ctrl.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        print(f"📩 JSON from {peer_fp[:16]}: {data}")
        await ctrl.send_json(peer_fp, {"status": "ok"})

    await ctrl.start()
    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await ctrl.stop()

asyncio.run(main())
```

### SubController

```python
import asyncio
from wire import SubController, MessageType

async def main():
    sub = SubController(controller_url="wss://127.0.0.1:8765", preshared_secret="my-secret")

    @sub.on(MessageType.JSON)
    async def on_json(data):
        print(f"📤 From controller: {data}")

    await sub.connect()
    await sub.send_json({"hello": "from python"})
    await sub.send_binary(b"hello" * 100)
    await sub.disconnect()

asyncio.run(main())
```

---

## Rust API / CLI usage 🦀

### Build release binary

```bash
cd rust/wire-rs
cargo build --release
```

### Run as CLI

```bash
cargo run -- controller --host 0.0.0.0 --port 8765 --secret my-secret
cargo run -- sub --host 127.0.0.1 --port 8765 --secret my-secret
```

Library usage examples are available in source modules and existing docs/tests.

---

## Testing matrix 🧪

### Python tests

```bash
cd python
source .venv/bin/activate   # if not already active
python -m pytest tests/ -v
```

### Rust tests

```bash
cd rust/wire-rs
cargo test -- --nocapture
```

### Cross-language interoperability tests

```bash
# build Rust binary first (required by tests)
cd rust/wire-rs
cargo build --release

cd ../python
source .venv/bin/activate
python -m pytest tests/test_cross_language.py -v
```

### Current passing status ✅

- Python suite: **57 passed, 5 skipped**
- Rust suites (`unit + integration + topology`): **73 passed**
- Cross-language tests: **5 passed**

---

## Reverse proxy 🔀

Wire includes a built-in **HTTP reverse proxy** (in both Python and Rust) that maps URL path prefixes to upstream HTTP services. It is useful for exposing multiple Docker containers or HTTP backends through a single entry point.

### Key features

- **Longest-prefix matching** — `/api/v2` is matched before `/api`
- **Prefix stripping** — `/api/users` with prefix `/api` is forwarded as `/users`
- **X-Forwarded-\* headers** — `X-Forwarded-For`, `X-Forwarded-Host`, `X-Forwarded-Proto`
- **Hop-by-hop header filtering** — connection-specific headers are stripped
- **Streaming response bodies** — large payloads are streamed efficiently
- **Dynamic route management** — add or remove routes at runtime

### Python quick start

```python
import asyncio
from wire import ReverseProxy

async def main():
    proxy = ReverseProxy(host="0.0.0.0", port=8080)
    proxy.add_route("/api", "http://backend:3000")
    proxy.add_route("/dashboard", "http://frontend:8081")
    await proxy.start()

    try:
        await asyncio.Future()  # run forever
    except KeyboardInterrupt:
        await proxy.stop()

asyncio.run(main())
```

### Rust quick start

```rust
use wire_rs::proxy::ReverseProxy;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut proxy = ReverseProxy::new("0.0.0.0", 8080);
    proxy.add_route("/api", "http://backend:3000").await;
    proxy.add_route("/dashboard", "http://frontend:8081").await;
    proxy.start().await?;

    tokio::signal::ctrl_c().await?;
    proxy.stop().await;
    Ok(())
}
```

### Integrated proxy on Controller / SubController

The reverse proxy can be **embedded directly** into a Controller or
SubController.  This is the recommended way to expose connected peers' HTTP
services through a single gateway.

```text
Browser / curl
    |
    v
Controller  (WSS :8765  +  HTTP proxy :8080)
    |            |            |
    v            v            v
SubCtrl-A    SubCtrl-B    SubCtrl-C
(API :3001)  (API :3002)  (Dashboard :3003)
```

#### Message-driven routing (recommended)

SubControllers configure routes remotely with **one call** — the Controller
needs **zero custom handler code**.  The `_wire_proxy_route` message is a
built-in Wire protocol feature, handled automatically.

**Python — Controller (only two lines needed):**

```python
ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="s3cret")
await ctrl.start()
await ctrl.enable_proxy(host="0.0.0.0", port=8080)   # that's it
```

**Python — SubController (one call configures everything):**

```python
sub = SubController(controller_url="wss://controller:8765", preshared_secret="s3cret")
await sub.connect()

# Tell the Controller: "route /my-api on your proxy to http://10.0.0.5:3001,
# and tie that route to MY fingerprint (auto-remove when I disconnect)"
await sub.request_proxy_route(
    "/my-api",                       # path prefix on the Controller's proxy
    sub.cert_bundle.fingerprint,     # bind to our own lifecycle
    "http://10.0.0.5:3001",         # where traffic actually goes
)
# GET http://controller:8080/my-api/health → http://10.0.0.5:3001/health
```

**Rust — same pattern:**

```rust
// Controller
let mut ctrl = Controller::new("0.0.0.0", 8765, "s3cret");
ctrl.start().await?;
ctrl.enable_proxy("0.0.0.0", 8080).await?;

// SubController
let mut sub = SubController::new("127.0.0.1", 8765, "s3cret");
sub.connect().await?;
let my_fp = sub.fingerprint().unwrap();
sub.request_proxy_route("/my-api", &my_fp, "http://10.0.0.5:3001").await?;
```

The Controller sends back a `_wire_proxy_route_result` confirmation with
`ok: true/false` and error details.

#### Programmatic API (for direct control)

When you need more control, the Controller can also manage routes
directly in code:

```python
# Static route (always active)
ctrl.add_proxy_route("/status", "http://monitoring:9090")

# Peer-bound route — auto-removed when the SubController disconnects
ctrl.add_proxy_route_for_peer("/worker-a/api", worker_a_fp, "http://10.0.0.5:3001")
```

### Proxy tunnels (HTTP through Wire mesh)

`open_proxy_tunnel()` creates a full **two-hop HTTP tunnel** through the
Wire mesh.  The calling node starts a local HTTP listener; requests are
serialised, sent through Wire to a target peer, and that peer makes the
actual HTTP call to the upstream backend.  Responses flow back the same
path.

```text
curl http://localhost:9090/api/users
    |
    v
SubController A  (HTTP listener :9090)
    |  <-- Wire mesh (relay or direct) -->
    v
SubController B  (forward proxy)
    |
    v
http://backend:3000/users  (the real service)
```

**Python — one call from any SubController or Controller:**

```python
# SubController A opens a tunnel:
# - listens on 0.0.0.0:9090
# - forwards /api/* through Wire to target peer B
# - B makes the HTTP call to http://10.0.0.5:3000/*
tunnel = await sub_a.open_proxy_tunnel(
    "0.0.0.0", 9090, "/api",
    target_fp=sub_b_fingerprint,
    upstream_url="http://10.0.0.5:3000",
)
# curl http://localhost:9090/api/health
#   → Wire → peer B → http://10.0.0.5:3000/health → response → Wire → client

# Close the tunnel when done
await sub_a.close_proxy_tunnel(tunnel)
```

The target peer handles `_wire_tunnel_req` messages **automatically** —
no code needed on the target side.  This works across Python and Rust
nodes (a Python SubController can tunnel through a Rust Controller and
vice versa).

### Use cases and examples

Runnable examples live in `examples/reverse-proxy/`:

| Example | Description |
|---|---|
| `basic_proxy` | Single-backend pass-through proxy |
| `multi_service_proxy` | Route multiple services by path prefix |
| `dynamic_routes_proxy` | Add and remove routes while the proxy is running |
| `versioned_api_proxy` | Route `/api/v1` and `/api/v2` to different backends |
| `wire_with_proxy` | Run a Wire WebSocket controller alongside the HTTP proxy |
| `controller_proxy_routing` | **Controller + zero-code proxy routing** |
| `subcontroller_announce` | **SubController configures routes remotely** |
| `tunnel_controller` | **Controller as tunnel target** |
| `tunnel_subcontroller` | **SubController opens a proxy tunnel through Wire** |

Python examples (`.py`) and Rust examples (`.rs`).

---

## Contribution checklist 📋

When changing protocol behavior:

1. Update tests in both implementations where relevant.
2. Run Python tests and Rust tests.
3. Run cross-language tests.
4. Keep behavior backward-compatible unless intentionally versioned.

## License

This project is provided as-is for internal/private use unless a LICENSE file is added explicitly.

---

## Interoperability claim 🤝

Python and Rust now share the same wire-format and framing model, with successful inter-op validation across controller/subcontroller roles.
