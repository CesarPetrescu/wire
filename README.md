# Wire

Bidirectional WebSocket communication framework with mutual TLS, pre-shared secret authentication, and certificate fingerprint pinning. Both a Python and Rust implementation are provided, sharing the same binary wire protocol so they can interoperate.

Designed for private LAN deployments where a Controller (server) and one or more SubControllers (clients) need to exchange JSON, binary blobs, files (including large zip archives up to 1GB+), and images over a single WebSocket connection.

## How it works

### Security model

1. On startup, each node generates an ECDSA P-256 self-signed certificate and private key.
2. When a SubController connects, it sends an AUTH frame containing the pre-shared secret and its certificate PEM.
3. The Controller verifies the secret. If it matches, the Controller responds with AUTH_OK and its own certificate PEM.
4. Both sides compute and store the SHA-256 fingerprint of the peer's certificate. On any future reconnection, the fingerprint is checked against the pinned value. A mismatch aborts the connection (possible MITM).

No external CA is needed. Trust is established via the shared secret on first contact, then maintained via fingerprint pinning.

### Wire protocol

All messages use a 24-byte binary header followed by the payload:

```
Offset  Size  Field
0       2     Magic (0xBE01)
2       1     Message type
3       16    Message ID (UUID)
19      1     Flags
20      4     Payload length
24      ...   Payload bytes
```

Message types:

| Code | Type      | Description                        |
|------|-----------|------------------------------------|
| 0x01 | JSON      | UTF-8 JSON payload                 |
| 0x02 | BINARY    | Arbitrary binary blob              |
| 0x03 | FILE      | File with name (2-byte name len + name + data) |
| 0x04 | IMAGE     | Image with name (same sub-header as FILE) |
| 0x10 | AUTH      | Authentication handshake           |
| 0x11 | AUTH_OK   | Authentication accepted            |
| 0x12 | AUTH_FAIL | Authentication rejected            |
| 0xFF | PING      | Keepalive                          |

Flags (bitmask):

| Bit | Flag         | Description                    |
|-----|--------------|--------------------------------|
| 0   | STREAM_START | First chunk of a streamed transfer |
| 1   | STREAM_CHUNK | Continuation chunk             |
| 2   | STREAM_END   | Final chunk                    |
| 3   | COMPRESSED   | Payload is zlib-compressed     |

Large payloads (over 4MB) are automatically split into streaming chunks. Compression is applied to payloads over 256 bytes when requested.

## Python implementation

### Requirements

- Python 3.11+
- Dependencies: `websockets`, `cryptography`

### Installation

```bash
cd python
pip install -r requirements.txt
```

### Usage

Controller (server):

```python
import asyncio
from wire import Controller, MessageType

async def main():
    ctrl = Controller(
        host="0.0.0.0",
        port=8765,
        preshared_secret="my-secret-key",
    )

    @ctrl.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        print(f"Received JSON from {peer_fp[:16]}: {data}")
        # Send a response back
        await ctrl.send_json(peer_fp, {"status": "ok"})

    @ctrl.on(MessageType.FILE)
    async def on_file(peer_fp, filename, data):
        print(f"Received file: {filename} ({len(data)} bytes)")

    @ctrl.on(MessageType.BINARY)
    async def on_binary(peer_fp, data):
        print(f"Received {len(data)} bytes of binary data")

    await ctrl.start()

    # Keep running
    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await ctrl.stop()

asyncio.run(main())
```

SubController (client):

```python
import asyncio
from wire import SubController, MessageType

async def main():
    sub = SubController(
        controller_url="wss://192.168.1.100:8765",
        preshared_secret="my-secret-key",
    )

    @sub.on(MessageType.JSON)
    async def on_json(data):
        print(f"Received from controller: {data}")

    await sub.connect()

    # Send various data types
    await sub.send_json({"sensor": "temp", "value": 23.5})
    await sub.send_binary(b"\x00\x01\x02\x03" * 1000)
    await sub.send_file("archive.zip", open("archive.zip", "rb").read())
    await sub.send_file("photo.png", open("photo.png", "rb").read(), is_image=True)

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await sub.disconnect()

asyncio.run(main())
```

### Running the Python tests

```bash
cd python
pip install -r requirements.txt
python -m pytest tests/ -v
```

This runs 31 tests covering:

- `test_protocol.py` (13 tests) -- Frame encoding/decoding, compression, stream flags, file payloads, error cases (bad magic, truncated frames).
- `test_certs.py` (6 tests) -- Certificate generation, fingerprint stability and uniqueness, SSL context creation.
- `test_integration.py` (12 tests) -- Full Controller+SubController over real WebSockets: authentication (success and rejection), JSON both directions, bidirectional simultaneous sends, binary blobs, small zip files (verified as valid zip archives), PNG image transfer, large binary streaming (10MB), large zip streaming (6MB, forcing chunked transfer), 100-message rapid fire with ordering verification, interleaved message types.

## Rust implementation

### Requirements

- Rust 1.70+ (tested with 1.93)
- All dependencies are managed via Cargo

### Building

```bash
cd rust/wire-rs
cargo build --release
```

### Usage as a library

Controller:

```rust
use wire_rs::controller::Controller;
use serde_json::json;

#[tokio::main]
async fn main() {
    let mut ctrl = Controller::new("0.0.0.0", 8765, "my-secret-key");
    let mut rx = ctrl.message_rx.take().unwrap();
    ctrl.start().await.unwrap();

    while let Some(msg) = rx.recv().await {
        match msg {
            wire_rs::controller::WireMessage::Json { peer_fp, data } => {
                println!("JSON from {}: {}", &peer_fp[..16], data);
                ctrl.send_json(&peer_fp, &json!({"ack": true})).await.unwrap();
            }
            wire_rs::controller::WireMessage::File { peer_fp, filename, data } => {
                println!("File: {} ({} bytes)", filename, data.len());
            }
            wire_rs::controller::WireMessage::Binary { peer_fp, data } => {
                println!("Binary: {} bytes", data.len());
            }
            wire_rs::controller::WireMessage::Image { peer_fp, filename, data } => {
                println!("Image: {} ({} bytes)", filename, data.len());
            }
        }
    }
}
```

SubController:

```rust
use wire_rs::subcontroller::SubController;
use serde_json::json;

#[tokio::main]
async fn main() {
    let mut sub = SubController::new("192.168.1.100", 8765, "my-secret-key");
    let mut rx = sub.message_rx.take().unwrap();
    sub.connect().await.unwrap();

    sub.send_json(&json!({"hello": "world"})).await.unwrap();
    sub.send_binary(&vec![0u8; 1024]).await.unwrap();
    sub.send_file("data.zip", &std::fs::read("data.zip").unwrap(), false).await.unwrap();
    sub.send_file("img.png", &std::fs::read("img.png").unwrap(), true).await.unwrap();

    while let Some(msg) = rx.recv().await {
        match msg {
            wire_rs::subcontroller::WireMessage::Json { data } => {
                println!("From controller: {}", data);
            }
            _ => {}
        }
    }
}
```

### Usage as a CLI

```bash
# Start the controller
cargo run -- controller --host 0.0.0.0 --port 8765 --secret my-secret

# In another terminal, start the subcontroller
cargo run -- sub --host 192.168.1.100 --port 8765 --secret my-secret
```

### Running the Rust tests

```bash
cd rust/wire-rs

# Run all tests (unit + integration)
cargo test

# Run only unit tests
cargo test --lib

# Run only integration tests
cargo test --test integration

# Run with output visible
cargo test -- --nocapture
```

This runs 24 tests covering:

Unit tests (15):
- Protocol: JSON/binary roundtrip, compression, stream flags, custom message IDs, bad magic detection, truncated frame handling, file payload encoding, empty payloads.
- Certs: Certificate generation, fingerprint stability and uniqueness, server/client TLS config creation.

Integration tests (9):
- Authentication success and bad-secret rejection.
- JSON from SubController to Controller and vice versa.
- Binary blob transfer (25.6KB).
- Zip file transfer with archive validity verification.
- PNG image transfer with header verification.
- Large binary streaming (10MB, forces chunked transfer).
- Rapid-fire 100 JSON messages with ordering verification.

## Project structure

```
wire/
  python/
    wire/
      __init__.py          # Package exports
      protocol.py          # Binary framing protocol
      certs.py             # Certificate generation and SSL contexts
      controller.py        # WebSocket server (Controller)
      subcontroller.py     # WebSocket client (SubController)
    tests/
      test_protocol.py     # Protocol unit tests
      test_certs.py        # Certificate unit tests
      test_integration.py  # Full end-to-end tests
    requirements.txt

  rust/wire-rs/
    src/
      lib.rs               # Module declarations
      protocol.rs          # Binary framing protocol
      certs.rs             # Certificate generation and TLS configs
      controller.rs        # WebSocket server (Controller)
      subcontroller.rs     # WebSocket client (SubController)
      main.rs              # CLI entry point
    tests/
      integration.rs       # Full end-to-end tests
    Cargo.toml
```

## Interoperability

Both implementations use the same wire protocol (magic bytes, header layout, message types, flags, compression, streaming chunk size). A Python Controller can talk to a Rust SubController and vice versa, as long as they share the same pre-shared secret.
