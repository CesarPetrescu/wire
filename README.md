# Wire

A bilingual (Python + Rust) framework for secure, high-performance WebSocket communication over a shared binary protocol.

Wire supports both:

- **Controller** (server) and **SubController** (client) roles
- **JSON**, **binary**, **file**, and **image** message types
- **TLS/mTLS-like trust** via certificate exchange + fingerprint pinning
- **Streaming / chunked transfers** for large payloads
- **Cross-language interoperability** (Python ↔ Rust)

---

## What Wire is for

Use Wire when you need private LAN / edge-style communication between one controller and many subcontrollers with strong peer identity checks and resumable large transfers.

---

## Security model (in short)

1. Each node generates an ECDSA P-256 self-signed certificate and key on startup.
2. SubController connects with a `AUTH` frame containing:
   - shared secret
   - its certificate
3. Controller validates the secret.
4. Controller replies with `AUTH_OK` and its certificate on success.
5. Both peers persist SHA-256 certificate fingerprints.
6. On reconnection, a mismatched pinned fingerprint is rejected (man-in-the-middle / identity mismatch protection).

No external CA is required.

---

## Wire protocol (binary)

All frames use a **24-byte header** + payload.

| Offset | Size | Field |
|---|---:|---|
| 0 | 2 | Magic (`0xBE01`) |
| 2 | 1 | Message type |
| 3 | 16 | Message ID (UUID) |
| 19 | 1 | Flags |
| 20 | 4 | Payload length |
| 24 | ... | Payload |

### Message types

| Code | Type | Meaning |
|---|---|---|
| `0x01` | `JSON` | UTF-8 JSON payload |
| `0x02` | `BINARY` | Arbitrary binary blob |
| `0x03` | `FILE` | File transfer (name + bytes) |
| `0x04` | `IMAGE` | Image transfer (name + bytes) |
| `0x10` | `AUTH` | Auth handshake |
| `0x11` | `AUTH_OK` | Handshake success |
| `0x12` | `AUTH_FAIL` | Handshake failure |
| `0xFF` | `PING` | Keepalive |

### Flags

| Bit | Flag | Meaning |
|---|---|---|
| 0 | `STREAM_START` | first chunk of stream |
| 1 | `STREAM_CHUNK` | intermediate chunk |
| 2 | `STREAM_END` | final chunk |
| 3 | `COMPRESSED` | payload compressed |

Large payloads are split into stream chunks; compression is applied when enabled.

---

## Repository layout

```text
wire/
  python/
    wire/                  # Library implementation
    tests/                 # Python tests
    requirements.txt

  rust/wire-rs/
    src/                   # Library + CLI implementation
    tests/                 # Rust tests
    Cargo.toml
```

---

## Python implementation

### Requirements

- Python 3.11+
- `websockets`, `cryptography`

### Setup

```bash
cd python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Quick usage

#### Controller

```python
import asyncio
from wire import Controller, MessageType

async def main():
    ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="my-secret-key")

    @ctrl.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        print(f"Received JSON from {peer_fp[:16]}: {data}")
        await ctrl.send_json(peer_fp, {"status": "ok"})

    await ctrl.start()
    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await ctrl.stop()

asyncio.run(main())
```

#### SubController

```python
import asyncio
from wire import SubController, MessageType

async def main():
    sub = SubController(controller_url="wss://192.168.1.100:8765", preshared_secret="my-secret-key")

    @sub.on(MessageType.JSON)
    async def on_json(data):
        print(f"Received from controller: {data}")

    await sub.connect()
    await sub.send_json({"sensor": "temp", "value": 23.5})
    await sub.send_binary(b"\x00\x01\x02\x03" * 1000)
    await sub.disconnect()

asyncio.run(main())
```

## Rust implementation

### Requirements

- Rust (1.70+)

### Build

```bash
cd rust/wire-rs
cargo build --release
```

### CLI example

```bash
# Controller
cargo run -- controller --host 0.0.0.0 --port 8765 --secret my-secret

# SubController (in another terminal)
cargo run -- sub --host 192.168.1.100 --port 8765 --secret my-secret
```

Library examples are in the existing README content and source docs under `rust/wire-rs/src`.

---

## Tests and verification

Run all Python tests:

```bash
cd /root/clawd/python
source .venv/bin/activate
python -m pytest tests/ -v
```

Run Rust tests:

```bash
cd rust/wire-rs
cargo test -- --nocapture
```

Run cross-language interoperability tests (requires Rust release binary built first):

```bash
cd rust/wire-rs
cargo build --release
cd ../python
source .venv/bin/activate
python -m pytest tests/test_cross_language.py -v
```

### Current status (this branch)

- Python test suite: **57 passed, 5 skipped**
- Rust unit/integration/star-topology tests: **73 passed**
- Cross-language tests: **5 passed**

---

## Interoperability

The Python and Rust stacks share the same binary protocol and message semantics.
They can interoperate as Controller/SubController in mixed-language setups when the same shared secret and certificate policy are used.
