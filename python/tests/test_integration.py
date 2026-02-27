"""
Integration tests — spin up Controller + SubController and verify
all message types flow bidirectionally over a single WebSocket.
"""

import asyncio
import io
import json
import os
import struct
import tempfile
import zipfile

import pytest
import pytest_asyncio

from wire.controller import Controller
from wire.subcontroller import SubController
from wire.protocol import MessageType

# Use a random high port to avoid collisions
TEST_PORT = 19876
SECRET = "test-preshared-secret-42"


@pytest_asyncio.fixture
async def wire_pair():
    """Fixture that starts a Controller + SubController pair."""
    ctrl_dir = tempfile.mkdtemp(prefix="wire_ctrl_")
    sub_dir = tempfile.mkdtemp(prefix="wire_sub_")

    controller = Controller(
        host="127.0.0.1",
        port=TEST_PORT,
        preshared_secret=SECRET,
        cert_dir=ctrl_dir,
    )
    sub = SubController(
        controller_url=f"wss://127.0.0.1:{TEST_PORT}",
        preshared_secret=SECRET,
        cert_dir=sub_dir,
    )

    await controller.start()
    await sub.connect()

    # Give the connection a moment to stabilize
    await asyncio.sleep(0.1)

    yield controller, sub

    await sub.disconnect()
    await controller.stop()


@pytest.mark.asyncio
async def test_auth_success(wire_pair):
    """Both sides should authenticate and have pinned fingerprints."""
    controller, sub = wire_pair
    assert sub.controller_fingerprint is not None
    assert len(controller.peer_fingerprints) == 1
    peer_fp = controller.peer_fingerprints[0]
    assert peer_fp == sub.cert_bundle.fingerprint


@pytest.mark.asyncio
async def test_auth_bad_secret():
    """Wrong pre-shared secret should be rejected."""
    ctrl_dir = tempfile.mkdtemp()
    sub_dir = tempfile.mkdtemp()
    port = TEST_PORT + 1

    controller = Controller(
        host="127.0.0.1", port=port, preshared_secret="correct", cert_dir=ctrl_dir
    )
    await controller.start()

    sub = SubController(
        controller_url=f"wss://127.0.0.1:{port}",
        preshared_secret="wrong",
        cert_dir=sub_dir,
    )

    with pytest.raises(ConnectionRefusedError, match="bad secret"):
        await sub.connect()

    await controller.stop()


@pytest.mark.asyncio
async def test_json_controller_to_sub(wire_pair):
    """Controller sends JSON, SubController receives it."""
    controller, sub = wire_pair
    received = asyncio.Event()
    result = {}

    @sub.on(MessageType.JSON)
    async def on_json(data):
        result.update(data)
        received.set()

    peer_fp = controller.peer_fingerprints[0]
    await controller.send_json(peer_fp, {"action": "ping", "value": 123})
    await asyncio.wait_for(received.wait(), timeout=5)
    assert result == {"action": "ping", "value": 123}


@pytest.mark.asyncio
async def test_json_sub_to_controller(wire_pair):
    """SubController sends JSON, Controller receives it."""
    controller, sub = wire_pair
    received = asyncio.Event()
    result = {}

    @controller.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        result.update(data)
        received.set()

    await sub.send_json({"status": "ready", "sensors": [1, 2, 3]})
    await asyncio.wait_for(received.wait(), timeout=5)
    assert result == {"status": "ready", "sensors": [1, 2, 3]}


@pytest.mark.asyncio
async def test_json_bidirectional(wire_pair):
    """Both sides send JSON simultaneously."""
    controller, sub = wire_pair
    ctrl_received = asyncio.Event()
    sub_received = asyncio.Event()
    ctrl_data = {}
    sub_data = {}

    @controller.on(MessageType.JSON)
    async def ctrl_handler(peer_fp, data):
        ctrl_data.update(data)
        ctrl_received.set()

    @sub.on(MessageType.JSON)
    async def sub_handler(data):
        sub_data.update(data)
        sub_received.set()

    peer_fp = controller.peer_fingerprints[0]
    await asyncio.gather(
        sub.send_json({"from": "sub"}),
        controller.send_json(peer_fp, {"from": "ctrl"}),
    )
    await asyncio.wait_for(
        asyncio.gather(ctrl_received.wait(), sub_received.wait()), timeout=5
    )
    assert ctrl_data == {"from": "sub"}
    assert sub_data == {"from": "ctrl"}


@pytest.mark.asyncio
async def test_binary_data(wire_pair):
    """Send raw binary data both directions."""
    controller, sub = wire_pair
    received = asyncio.Event()
    result = bytearray()

    @controller.on(MessageType.BINARY)
    async def on_binary(peer_fp, data):
        result.extend(data)
        received.set()

    blob = bytes(range(256)) * 100  # 25.6 KB of binary
    await sub.send_binary(blob)
    await asyncio.wait_for(received.wait(), timeout=5)
    assert bytes(result) == blob


@pytest.mark.asyncio
async def test_send_small_zip(wire_pair):
    """Send a small zip file from SubController to Controller."""
    controller, sub = wire_pair
    received = asyncio.Event()
    file_result = {"name": None, "data": None}

    @controller.on(MessageType.FILE)
    async def on_file(peer_fp, filename, data):
        file_result["name"] = filename
        file_result["data"] = data
        received.set()

    # Create a real zip in memory
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("hello.txt", "Hello from wire!")
        zf.writestr("data.json", json.dumps({"key": "value"}))
    zip_bytes = buf.getvalue()

    await sub.send_file("test_archive.zip", zip_bytes)
    await asyncio.wait_for(received.wait(), timeout=5)

    assert file_result["name"] == "test_archive.zip"
    # Verify the zip is valid
    received_zip = zipfile.ZipFile(io.BytesIO(file_result["data"]))
    assert received_zip.read("hello.txt") == b"Hello from wire!"
    assert json.loads(received_zip.read("data.json")) == {"key": "value"}


@pytest.mark.asyncio
async def test_send_image(wire_pair):
    """Send an image (PNG-like data) from Controller to SubController."""
    controller, sub = wire_pair
    received = asyncio.Event()
    img_result = {"name": None, "data": None}

    @sub.on(MessageType.IMAGE)
    async def on_image(filename, data):
        img_result["name"] = filename
        img_result["data"] = data
        received.set()

    # Create a minimal valid PNG (1x1 red pixel)
    png_data = _make_tiny_png()

    peer_fp = controller.peer_fingerprints[0]
    await controller.send_file(peer_fp, "photo.png", png_data, is_image=True)
    await asyncio.wait_for(received.wait(), timeout=5)

    assert img_result["name"] == "photo.png"
    assert img_result["data"] == png_data
    assert img_result["data"][:8] == b"\x89PNG\r\n\x1a\n"


@pytest.mark.asyncio
async def test_large_binary_streaming(wire_pair):
    """Send a large binary blob that triggers chunk streaming."""
    controller, sub = wire_pair
    received = asyncio.Event()
    result = bytearray()

    @controller.on(MessageType.BINARY)
    async def on_binary(peer_fp, data):
        result.extend(data)
        received.set()

    # 10 MB of data — will be streamed in chunks
    big_blob = os.urandom(10 * 1024 * 1024)
    await sub.send_binary(big_blob)
    await asyncio.wait_for(received.wait(), timeout=30)
    assert bytes(result) == big_blob


@pytest.mark.asyncio
async def test_large_zip_streaming(wire_pair):
    """Send a large zip file that requires streaming (> 4MB chunk size)."""
    controller, sub = wire_pair
    received = asyncio.Event()
    file_result = {"name": None, "data": None}

    @controller.on(MessageType.FILE)
    async def on_file(peer_fp, filename, data):
        file_result["name"] = filename
        file_result["data"] = data
        received.set()

    # Create a ~6 MB zip to force streaming
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("big_file.bin", os.urandom(6 * 1024 * 1024))
    zip_bytes = buf.getvalue()

    await sub.send_file("big_archive.zip", zip_bytes)
    await asyncio.wait_for(received.wait(), timeout=30)

    assert file_result["name"] == "big_archive.zip"
    received_zip = zipfile.ZipFile(io.BytesIO(file_result["data"]))
    assert "big_file.bin" in received_zip.namelist()


@pytest.mark.asyncio
async def test_multiple_json_rapid_fire(wire_pair):
    """Send many JSON messages rapidly and verify all arrive."""
    controller, sub = wire_pair
    count = 100
    results = []
    done = asyncio.Event()

    @controller.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        results.append(data)
        if len(results) >= count:
            done.set()

    for i in range(count):
        await sub.send_json({"seq": i})

    await asyncio.wait_for(done.wait(), timeout=10)
    assert len(results) == count
    seqs = sorted(r["seq"] for r in results)
    assert seqs == list(range(count))


@pytest.mark.asyncio
async def test_interleaved_types(wire_pair):
    """Send JSON, binary, and file messages interleaved."""
    controller, sub = wire_pair
    json_results = []
    binary_results = []
    file_results = []
    all_done = asyncio.Event()
    expected = 3

    @controller.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        json_results.append(data)
        _check_done()

    @controller.on(MessageType.BINARY)
    async def on_binary(peer_fp, data):
        binary_results.append(data)
        _check_done()

    @controller.on(MessageType.FILE)
    async def on_file(peer_fp, filename, data):
        file_results.append((filename, data))
        _check_done()

    def _check_done():
        if len(json_results) + len(binary_results) + len(file_results) >= expected:
            all_done.set()

    await sub.send_json({"type": "config"})
    await sub.send_binary(b"\xDE\xAD\xBE\xEF" * 256)
    await sub.send_file("data.bin", b"\x00" * 1000)

    await asyncio.wait_for(all_done.wait(), timeout=10)
    assert json_results == [{"type": "config"}]
    assert binary_results == [b"\xDE\xAD\xBE\xEF" * 256]
    assert file_results[0][0] == "data.bin"


# -- helpers ----------------------------------------------------------------

def _make_tiny_png() -> bytes:
    """Create a minimal 1x1 red PNG."""
    import struct
    import zlib

    def _chunk(chunk_type: bytes, data: bytes) -> bytes:
        c = chunk_type + data
        crc = struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack(">I", len(data)) + c + crc

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    # Raw pixel: filter=0, R=255, G=0, B=0
    raw = zlib.compress(b"\x00\xff\x00\x00")
    idat = _chunk(b"IDAT", raw)
    iend = _chunk(b"IEND", b"")
    return sig + ihdr + idat + iend
