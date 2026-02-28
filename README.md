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
  python/
    wire/
      __init__.py
      protocol.py
      certs.py
      controller.py
      subcontroller.py
    tests/
      test_protocol.py
      test_certs.py
      test_integration.py
      test_cross_language.py
      test_star_topology.py
    requirements.txt

  rust/wire-rs/
    src/
      lib.rs
      protocol.rs
      certs.rs
      controller.rs
      subcontroller.rs
      main.rs
    tests/
      lib.rs
      integration.rs
      star_topology.rs
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
