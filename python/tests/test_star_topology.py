"""
Star topology integration tests — 1 Controller + 5 SubControllers.

Verifies:
  - All 5 subs connect and authenticate
  - Peer discovery: each sub learns about the other 4
  - Peer-to-peer relay: JSON, binary, file, and image via Controller
  - Peer leave notifications when a sub disconnects
"""

import asyncio
import hashlib
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

TEST_PORT = 19900
SECRET = "star-test-secret-77"
NUM_SUBS = 5


@pytest_asyncio.fixture
async def star():
    """Fixture that starts a Controller + 5 SubControllers."""
    ctrl_dir = tempfile.mkdtemp(prefix="wire_ctrl_star_")
    sub_dirs = [tempfile.mkdtemp(prefix=f"wire_sub{i}_") for i in range(NUM_SUBS)]

    controller = Controller(
        host="127.0.0.1",
        port=TEST_PORT,
        preshared_secret=SECRET,
        cert_dir=ctrl_dir,
    )
    await controller.start()

    subs = []
    for i in range(NUM_SUBS):
        sub = SubController(
            controller_url=f"wss://127.0.0.1:{TEST_PORT}",
            preshared_secret=SECRET,
            cert_dir=sub_dirs[i],
        )
        await sub.connect()
        # Small delay so peer notifications propagate
        await asyncio.sleep(0.1)
        subs.append(sub)

    # Let peer notifications finish propagating
    await asyncio.sleep(0.3)

    yield controller, subs

    for sub in subs:
        await sub.disconnect()
    await controller.stop()


@pytest.mark.asyncio
async def test_all_subs_connected(star):
    """All 5 SubControllers should be connected to the Controller."""
    controller, subs = star
    assert len(controller.peer_fingerprints) == NUM_SUBS
    for sub in subs:
        assert sub.controller_fingerprint is not None


@pytest.mark.asyncio
async def test_peer_discovery(star):
    """Each SubController should know about the other 4 peers."""
    controller, subs = star
    for i, sub in enumerate(subs):
        peers = sub.known_peers
        assert len(peers) == NUM_SUBS - 1, (
            f"Sub {i} has {len(peers)} peers, expected {NUM_SUBS - 1}"
        )
        # Should not contain its own fingerprint
        assert sub.cert_bundle.fingerprint not in peers
        # Should contain all other subs' fingerprints
        for j, other in enumerate(subs):
            if i != j:
                assert other.cert_bundle.fingerprint in peers


@pytest.mark.asyncio
async def test_relay_json(star):
    """Sub 0 sends JSON to Sub 1 via relay."""
    controller, subs = star
    received = asyncio.Event()
    result = {}

    @subs[1].on_relay(MessageType.JSON)
    async def on_json(source_fp, data):
        result["source"] = source_fp
        result["data"] = data
        received.set()

    dest_fp = subs[1].cert_bundle.fingerprint
    await subs[0].send_json_to_peer(dest_fp, {"msg": "hello from sub0", "num": 42})
    await asyncio.wait_for(received.wait(), timeout=5)

    assert result["source"] == subs[0].cert_bundle.fingerprint
    assert result["data"] == {"msg": "hello from sub0", "num": 42}


@pytest.mark.asyncio
async def test_relay_binary(star):
    """Sub 2 sends binary to Sub 3 via relay."""
    controller, subs = star
    received = asyncio.Event()
    result = {}

    @subs[3].on_relay(MessageType.BINARY)
    async def on_binary(source_fp, data):
        result["source"] = source_fp
        result["data"] = data
        received.set()

    blob = bytes(range(256)) * 100  # 25.6 KB
    dest_fp = subs[3].cert_bundle.fingerprint
    await subs[2].send_binary_to_peer(dest_fp, blob)
    await asyncio.wait_for(received.wait(), timeout=5)

    assert result["source"] == subs[2].cert_bundle.fingerprint
    assert result["data"] == blob


@pytest.mark.asyncio
async def test_relay_file_with_checksum(star):
    """Sub 1 sends a zip file to Sub 4 via relay, with checksum validation."""
    controller, subs = star
    received = asyncio.Event()
    result = {}

    @subs[4].on_relay(MessageType.FILE)
    async def on_file(source_fp, filename, data):
        result["source"] = source_fp
        result["filename"] = filename
        result["data"] = data
        received.set()

    # Create a real zip
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("relayed.txt", "This file was relayed through the Controller!")
    zip_bytes = buf.getvalue()

    dest_fp = subs[4].cert_bundle.fingerprint
    await subs[1].send_file_to_peer(dest_fp, "relay_test.zip", zip_bytes)
    await asyncio.wait_for(received.wait(), timeout=5)

    assert result["source"] == subs[1].cert_bundle.fingerprint
    assert result["filename"] == "relay_test.zip"
    assert result["data"] == zip_bytes
    # Verify the zip is still valid after relay
    received_zip = zipfile.ZipFile(io.BytesIO(result["data"]))
    assert received_zip.read("relayed.txt") == b"This file was relayed through the Controller!"


@pytest.mark.asyncio
async def test_relay_image(star):
    """Sub 3 sends an image to Sub 0 via relay."""
    controller, subs = star
    received = asyncio.Event()
    result = {}

    @subs[0].on_relay(MessageType.IMAGE)
    async def on_image(source_fp, filename, data):
        result["source"] = source_fp
        result["filename"] = filename
        result["data"] = data
        received.set()

    png_data = _make_tiny_png()
    dest_fp = subs[0].cert_bundle.fingerprint
    await subs[3].send_file_to_peer(dest_fp, "relayed.png", png_data, is_image=True)
    await asyncio.wait_for(received.wait(), timeout=5)

    assert result["source"] == subs[3].cert_bundle.fingerprint
    assert result["filename"] == "relayed.png"
    assert result["data"] == png_data
    assert result["data"][:8] == b"\x89PNG\r\n\x1a\n"


@pytest.mark.asyncio
async def test_broadcast_relay_to_all_peers(star):
    """Sub 0 sends JSON to all other 4 subs via relay."""
    controller, subs = star
    received_count = 0
    results = {}
    all_received = asyncio.Event()

    for i in range(1, NUM_SUBS):
        @subs[i].on_relay(MessageType.JSON)
        async def on_json(source_fp, data, idx=i):
            nonlocal received_count
            results[idx] = {"source": source_fp, "data": data}
            received_count += 1
            if received_count >= NUM_SUBS - 1:
                all_received.set()

    # Sub 0 sends to all other subs
    for i in range(1, NUM_SUBS):
        dest_fp = subs[i].cert_bundle.fingerprint
        await subs[0].send_json_to_peer(dest_fp, {"broadcast": True, "from": 0})

    await asyncio.wait_for(all_received.wait(), timeout=10)

    assert len(results) == NUM_SUBS - 1
    for i in range(1, NUM_SUBS):
        assert results[i]["source"] == subs[0].cert_bundle.fingerprint
        assert results[i]["data"] == {"broadcast": True, "from": 0}


@pytest.mark.asyncio
async def test_all_pairs_can_relay(star):
    """Every sub sends a JSON message to every other sub (20 messages total)."""
    controller, subs = star
    expected_total = NUM_SUBS * (NUM_SUBS - 1)  # 20
    received_count = 0
    results = []
    all_received = asyncio.Event()

    for i in range(NUM_SUBS):
        @subs[i].on_relay(MessageType.JSON)
        async def on_json(source_fp, data, idx=i):
            nonlocal received_count
            results.append({"receiver": idx, "source": source_fp, "data": data})
            received_count += 1
            if received_count >= expected_total:
                all_received.set()

    # Every sub sends to every other sub
    for i in range(NUM_SUBS):
        for j in range(NUM_SUBS):
            if i != j:
                dest_fp = subs[j].cert_bundle.fingerprint
                await subs[i].send_json_to_peer(dest_fp, {"from": i, "to": j})

    await asyncio.wait_for(all_received.wait(), timeout=15)

    assert len(results) == expected_total
    # Verify each receiver got messages from all other subs
    for i in range(NUM_SUBS):
        msgs_for_i = [r for r in results if r["receiver"] == i]
        assert len(msgs_for_i) == NUM_SUBS - 1


@pytest.mark.asyncio
async def test_peer_leave_notification(star):
    """When a sub disconnects, others should be notified."""
    controller, subs = star
    leaving_fp = subs[4].cert_bundle.fingerprint

    # All subs should currently know about sub 4
    for i in range(4):
        assert leaving_fp in subs[i].known_peers

    # Disconnect sub 4
    await subs[4].disconnect()
    await asyncio.sleep(0.5)

    # Other subs should no longer see sub 4
    for i in range(4):
        assert leaving_fp not in subs[i].known_peers

    # Controller should have 4 peers now
    assert len(controller.peer_fingerprints) == NUM_SUBS - 1

    # Re-add sub 4 to subs list for cleanup (already disconnected)
    # We need to prevent the fixture from trying to disconnect again
    subs[4]._ws = None
    subs[4]._listen_task = None


@pytest.mark.asyncio
async def test_controller_sends_to_specific_sub(star):
    """Controller can still send directly to any specific SubController."""
    controller, subs = star
    received = asyncio.Event()
    result = {}

    @subs[2].on(MessageType.JSON)
    async def on_json(data):
        result.update(data)
        received.set()

    fp = subs[2].cert_bundle.fingerprint
    await controller.send_json(fp, {"direct": True, "target": 2})
    await asyncio.wait_for(received.wait(), timeout=5)
    assert result == {"direct": True, "target": 2}


@pytest.mark.asyncio
async def test_sub_sends_to_controller(star):
    """SubControllers can still send directly to the Controller."""
    controller, subs = star
    received = asyncio.Event()
    result = {}

    @controller.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        result["fp"] = peer_fp
        result["data"] = data
        received.set()

    await subs[3].send_json({"status": "alive", "sub_id": 3})
    await asyncio.wait_for(received.wait(), timeout=5)
    assert result["fp"] == subs[3].cert_bundle.fingerprint
    assert result["data"] == {"status": "alive", "sub_id": 3}


# -- helpers ----------------------------------------------------------------

def _make_tiny_png() -> bytes:
    """Create a minimal 1x1 red PNG."""
    import zlib

    def _chunk(chunk_type: bytes, data: bytes) -> bytes:
        c = chunk_type + data
        crc = struct.pack(">I", zlib.crc32(c) & 0xFFFFFFFF)
        return struct.pack(">I", len(data)) + c + crc

    sig = b"\x89PNG\r\n\x1a\n"
    ihdr = _chunk(b"IHDR", struct.pack(">IIBBBBB", 1, 1, 8, 2, 0, 0, 0))
    raw = zlib.compress(b"\x00\xff\x00\x00")
    idat = _chunk(b"IDAT", raw)
    iend = _chunk(b"IEND", b"")
    return sig + ihdr + idat + iend
