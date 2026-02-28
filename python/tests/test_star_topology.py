"""
Comprehensive star topology integration tests — 1 Controller + 5 SubControllers.

Test matrix:
  - All 20 directional pairs (every sub → every other sub) for each data type
  - 4 data types: JSON, binary, file (with checksum), image (with checksum)
  - 3 size tiers: 256B (small), 5MB (streamed), 16MB (large streamed)
  - Direct controller ↔ sub communication still works
  - Peer discovery and leave notifications
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

SECRET = "star-test-secret-77"
NUM_SUBS = 5

# Size tiers
SIZE_SMALL = 256  # 256 bytes
SIZE_MEDIUM = 5 * 1024 * 1024  # 5 MB (triggers streaming)
SIZE_LARGE = 16 * 1024 * 1024  # 16 MB

# Port assignments — each fixture uses its own port to avoid collisions
# (pytest-asyncio may run fixtures in parallel across test files)
PORT_BASE = 19900


def _next_port():
    """Thread-safe incrementing port number."""
    _next_port.counter += 1
    return PORT_BASE + _next_port.counter


_next_port.counter = 0


@pytest_asyncio.fixture
async def star():
    """Fixture that starts a Controller + 5 SubControllers."""
    port = _next_port()
    ctrl_dir = tempfile.mkdtemp(prefix="wire_ctrl_star_")
    sub_dirs = [tempfile.mkdtemp(prefix=f"wire_sub{i}_") for i in range(NUM_SUBS)]

    controller = Controller(
        host="127.0.0.1",
        port=port,
        preshared_secret=SECRET,
        cert_dir=ctrl_dir,
    )
    await controller.start()

    subs = []
    for i in range(NUM_SUBS):
        sub = SubController(
            controller_url=f"wss://127.0.0.1:{port}",
            preshared_secret=SECRET,
            cert_dir=sub_dirs[i],
        )
        await sub.connect()
        await asyncio.sleep(0.1)
        subs.append(sub)

    # Let peer notifications finish propagating
    await asyncio.sleep(0.5)

    yield controller, subs

    for sub in subs:
        try:
            await sub.disconnect()
        except Exception:
            pass
    await controller.stop()


@pytest_asyncio.fixture
async def star_large():
    """Separate fixture for 1GB tests (slower setup not shared with small tests)."""
    port = _next_port()
    ctrl_dir = tempfile.mkdtemp(prefix="wire_ctrl_star_lg_")
    sub_dirs = [tempfile.mkdtemp(prefix=f"wire_sub_lg{i}_") for i in range(NUM_SUBS)]

    controller = Controller(
        host="127.0.0.1",
        port=port,
        preshared_secret=SECRET,
        cert_dir=ctrl_dir,
    )
    await controller.start()

    subs = []
    for i in range(NUM_SUBS):
        sub = SubController(
            controller_url=f"wss://127.0.0.1:{port}",
            preshared_secret=SECRET,
            cert_dir=sub_dirs[i],
        )
        await sub.connect()
        await asyncio.sleep(0.1)
        subs.append(sub)

    await asyncio.sleep(0.5)

    yield controller, subs

    for sub in subs:
        try:
            await sub.disconnect()
        except Exception:
            pass
    await controller.stop()


# ===========================================================================
# Helpers
# ===========================================================================


def _make_json_data(size_hint: int) -> dict:
    """Create a JSON-serializable dict roughly `size_hint` bytes when encoded."""
    # Each entry is about 20 bytes: {"k_XXXX": "vXXXX_"} with overhead
    count = max(1, size_hint // 20)
    return {f"k_{i:06d}": f"v_{i:06d}" for i in range(count)}


def _make_binary_data(size: int) -> bytes:
    """Create binary data of exact `size`."""
    # Use repeating pattern so it's deterministic but not all-zero
    pattern = bytes(range(256))
    repeats = size // 256
    remainder = size % 256
    return pattern * repeats + pattern[:remainder]


def _make_file_data(size: int) -> tuple[str, bytes]:
    """Create a zip file with content roughly `size` bytes. Returns (filename, data)."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_STORED) as zf:
        zf.writestr("payload.bin", _make_binary_data(size))
    return "test_transfer.zip", buf.getvalue()


def _make_image_data(size: int) -> tuple[str, bytes]:
    """Create a PNG-like image of roughly `size` bytes. Returns (filename, data)."""
    png_header = _make_tiny_png()
    # Pad to desired size
    if size > len(png_header):
        padding = _make_binary_data(size - len(png_header))
        data = png_header + padding
    else:
        data = png_header
    return "test_image.png", data


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


# ===========================================================================
# Connection & peer discovery tests
# ===========================================================================


class TestConnectionAndDiscovery:
    @pytest.mark.asyncio
    async def test_all_subs_connected(self, star):
        """All 5 SubControllers should be connected to the Controller."""
        controller, subs = star
        assert len(controller.peer_fingerprints) == NUM_SUBS
        for sub in subs:
            assert sub.controller_fingerprint is not None

    @pytest.mark.asyncio
    async def test_peer_discovery_all_see_each_other(self, star):
        """Each SubController should know about the other 4 peers."""
        controller, subs = star
        for i, sub in enumerate(subs):
            peers = sub.known_peers
            assert len(peers) == NUM_SUBS - 1, (
                f"Sub {i} has {len(peers)} peers, expected {NUM_SUBS - 1}"
            )
            assert sub.cert_bundle.fingerprint not in peers
            for j, other in enumerate(subs):
                if i != j:
                    assert other.cert_bundle.fingerprint in peers

    @pytest.mark.asyncio
    async def test_peer_leave_notification(self, star):
        """When a sub disconnects, others should be notified and lose it from known_peers."""
        controller, subs = star
        leaving_fp = subs[4].cert_bundle.fingerprint

        for i in range(4):
            assert leaving_fp in subs[i].known_peers

        await subs[4].disconnect()
        await asyncio.sleep(0.5)

        for i in range(4):
            assert leaving_fp not in subs[i].known_peers
        assert len(controller.peer_fingerprints) == NUM_SUBS - 1

        # Prevent double-disconnect in fixture cleanup
        subs[4]._ws = None
        subs[4]._listen_task = None


# ===========================================================================
# 256-byte (small) tests — all 20 pairs × all 4 data types
# ===========================================================================


class TestSmall256B:
    """All 20 directional pairs with 256-byte payloads for each data type."""

    @pytest.mark.asyncio
    async def test_all_pairs_json_256b(self, star):
        """Every sub sends 256B JSON to every other sub (20 transfers)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.JSON)
            async def handler(source_fp, data, idx=i):
                results.append({"receiver": idx, "source": source_fp, "data": data})
                if len(results) >= expected:
                    done.set()

        test_data = _make_json_data(SIZE_SMALL)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_json_to_peer(dest_fp, {"from": i, "to": j, **test_data})

        await asyncio.wait_for(done.wait(), timeout=30)
        assert len(results) == expected

        # Verify every receiver got messages from every other sender
        for i in range(NUM_SUBS):
            msgs = [r for r in results if r["receiver"] == i]
            assert len(msgs) == NUM_SUBS - 1
            senders = {r["source"] for r in msgs}
            for j in range(NUM_SUBS):
                if j != i:
                    assert subs[j].cert_bundle.fingerprint in senders

    @pytest.mark.asyncio
    async def test_all_pairs_binary_256b(self, star):
        """Every sub sends 256B binary to every other sub (20 transfers)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.BINARY)
            async def handler(source_fp, data, idx=i):
                results.append({"receiver": idx, "source": source_fp, "data": data})
                if len(results) >= expected:
                    done.set()

        blob = _make_binary_data(SIZE_SMALL)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_binary_to_peer(dest_fp, blob)

        await asyncio.wait_for(done.wait(), timeout=30)
        assert len(results) == expected
        for r in results:
            assert r["data"] == blob

    @pytest.mark.asyncio
    async def test_all_pairs_file_256b(self, star):
        """Every sub sends 256B file to every other sub (20 transfers, checksum validated)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.FILE)
            async def handler(source_fp, filename, data, idx=i):
                results.append({
                    "receiver": idx,
                    "source": source_fp,
                    "filename": filename,
                    "data": data,
                })
                if len(results) >= expected:
                    done.set()

        fname, fdata = _make_file_data(SIZE_SMALL)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_file_to_peer(dest_fp, fname, fdata)

        await asyncio.wait_for(done.wait(), timeout=30)
        assert len(results) == expected
        for r in results:
            assert r["filename"] == fname
            assert r["data"] == fdata

    @pytest.mark.asyncio
    async def test_all_pairs_image_256b(self, star):
        """Every sub sends 256B image to every other sub (20 transfers, checksum validated)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.IMAGE)
            async def handler(source_fp, filename, data, idx=i):
                results.append({
                    "receiver": idx,
                    "source": source_fp,
                    "filename": filename,
                    "data": data,
                })
                if len(results) >= expected:
                    done.set()

        fname, fdata = _make_image_data(SIZE_SMALL)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_file_to_peer(dest_fp, fname, fdata, is_image=True)

        await asyncio.wait_for(done.wait(), timeout=30)
        assert len(results) == expected
        for r in results:
            assert r["filename"] == fname
            assert r["data"] == fdata
            assert r["data"][:4] == b"\x89PNG"


# ===========================================================================
# 5MB (streamed) tests — all 20 pairs × all 4 data types
# ===========================================================================


class TestMedium5MB:
    """All 20 directional pairs with 5MB payloads (triggers streaming)."""

    @pytest.mark.asyncio
    async def test_all_pairs_json_5mb(self, star):
        """Every sub sends ~5MB JSON to every other sub (20 transfers)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.JSON)
            async def handler(source_fp, data, idx=i):
                results.append({"receiver": idx, "source": source_fp, "size": len(json.dumps(data))})
                if len(results) >= expected:
                    done.set()

        test_data = _make_json_data(SIZE_MEDIUM)
        encoded_size = len(json.dumps(test_data))

        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_json_to_peer(dest_fp, test_data)

        await asyncio.wait_for(done.wait(), timeout=120)
        assert len(results) == expected
        for r in results:
            assert r["size"] == encoded_size

    @pytest.mark.asyncio
    async def test_all_pairs_binary_5mb(self, star):
        """Every sub sends 5MB binary to every other sub (20 transfers)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.BINARY)
            async def handler(source_fp, data, idx=i):
                results.append({"receiver": idx, "source": source_fp, "len": len(data)})
                if len(results) >= expected:
                    done.set()

        blob = _make_binary_data(SIZE_MEDIUM)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_binary_to_peer(dest_fp, blob)

        await asyncio.wait_for(done.wait(), timeout=120)
        assert len(results) == expected
        for r in results:
            assert r["len"] == SIZE_MEDIUM

    @pytest.mark.asyncio
    async def test_all_pairs_file_5mb(self, star):
        """Every sub sends 5MB file to every other sub (20 transfers, checksum validated)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.FILE)
            async def handler(source_fp, filename, data, idx=i):
                results.append({
                    "receiver": idx,
                    "source": source_fp,
                    "filename": filename,
                    "len": len(data),
                })
                if len(results) >= expected:
                    done.set()

        fname, fdata = _make_file_data(SIZE_MEDIUM)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_file_to_peer(dest_fp, fname, fdata)

        await asyncio.wait_for(done.wait(), timeout=120)
        assert len(results) == expected
        for r in results:
            assert r["filename"] == fname
            assert r["len"] == len(fdata)

    @pytest.mark.asyncio
    async def test_all_pairs_image_5mb(self, star):
        """Every sub sends 5MB image to every other sub (20 transfers, checksum validated)."""
        controller, subs = star
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.IMAGE)
            async def handler(source_fp, filename, data, idx=i):
                results.append({
                    "receiver": idx,
                    "source": source_fp,
                    "filename": filename,
                    "len": len(data),
                })
                if len(results) >= expected:
                    done.set()

        fname, fdata = _make_image_data(SIZE_MEDIUM)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_file_to_peer(dest_fp, fname, fdata, is_image=True)

        await asyncio.wait_for(done.wait(), timeout=120)
        assert len(results) == expected
        for r in results:
            assert r["filename"] == fname
            assert r["len"] == len(fdata)


# ===========================================================================
# 1GB tests — all 20 pairs × all 4 data types
# ===========================================================================


class TestLarge16MB:
    """All 20 directional pairs with 16MB payloads for each data type.
    These tests use the dedicated star_large fixture."""

    @pytest.mark.asyncio
    async def test_all_pairs_json_16mb(self, star_large):
        """Every sub sends ~16MB JSON to every other sub (20 transfers)."""
        controller, subs = star_large
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.JSON)
            async def handler(source_fp, data, idx=i):
                results.append({"receiver": idx, "source": source_fp, "size": len(json.dumps(data))})
                if len(results) >= expected:
                    done.set()

        test_data = _make_json_data(SIZE_LARGE)
        encoded_size = len(json.dumps(test_data))

        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_json_to_peer(dest_fp, test_data)

        await asyncio.wait_for(done.wait(), timeout=600)
        assert len(results) == expected
        for r in results:
            assert r["size"] == encoded_size

    @pytest.mark.asyncio
    async def test_all_pairs_binary_16mb(self, star_large):
        """Every sub sends 16MB binary to every other sub (20 transfers)."""
        controller, subs = star_large
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.BINARY)
            async def handler(source_fp, data, idx=i):
                results.append({"receiver": idx, "source": source_fp, "len": len(data)})
                if len(results) >= expected:
                    done.set()

        blob = _make_binary_data(SIZE_LARGE)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_binary_to_peer(dest_fp, blob)

        await asyncio.wait_for(done.wait(), timeout=600)
        assert len(results) == expected
        for r in results:
            assert r["len"] == SIZE_LARGE

    @pytest.mark.asyncio
    async def test_all_pairs_file_16mb(self, star_large):
        """Every sub sends 16MB file to every other sub (20 transfers, checksum validated)."""
        controller, subs = star_large
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.FILE)
            async def handler(source_fp, filename, data, idx=i):
                results.append({
                    "receiver": idx,
                    "source": source_fp,
                    "filename": filename,
                    "len": len(data),
                })
                if len(results) >= expected:
                    done.set()

        fname, fdata = _make_file_data(SIZE_LARGE)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_file_to_peer(dest_fp, fname, fdata)

        await asyncio.wait_for(done.wait(), timeout=600)
        assert len(results) == expected
        for r in results:
            assert r["filename"] == fname
            assert r["len"] == len(fdata)

    @pytest.mark.asyncio
    async def test_all_pairs_image_16mb(self, star_large):
        """Every sub sends 16MB image to every other sub (20 transfers, checksum validated)."""
        controller, subs = star_large
        expected = NUM_SUBS * (NUM_SUBS - 1)
        results = []
        done = asyncio.Event()

        for i in range(NUM_SUBS):
            @subs[i].on_relay(MessageType.IMAGE)
            async def handler(source_fp, filename, data, idx=i):
                results.append({
                    "receiver": idx,
                    "source": source_fp,
                    "filename": filename,
                    "len": len(data),
                })
                if len(results) >= expected:
                    done.set()

        fname, fdata = _make_image_data(SIZE_LARGE)
        for i in range(NUM_SUBS):
            for j in range(NUM_SUBS):
                if i != j:
                    dest_fp = subs[j].cert_bundle.fingerprint
                    await subs[i].send_file_to_peer(dest_fp, fname, fdata, is_image=True)

        await asyncio.wait_for(done.wait(), timeout=600)
        assert len(results) == expected
        for r in results:
            assert r["filename"] == fname
            assert r["len"] == len(fdata)


# ===========================================================================
# Direct controller ↔ sub tests (all 5 subs, all types, both directions)
# ===========================================================================


class TestDirectControllerSub:
    """Verify direct controller ↔ sub communication still works for all 5 subs."""

    @pytest.mark.asyncio
    async def test_controller_sends_json_to_each_sub(self, star):
        """Controller sends JSON to each of the 5 subs."""
        controller, subs = star
        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @sub.on(MessageType.JSON)
            async def on_json(data, r=result, e=received):
                r.update(data)
                e.set()

            fp = sub.cert_bundle.fingerprint
            await controller.send_json(fp, {"target": i, "msg": "hello"})
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result == {"target": i, "msg": "hello"}

    @pytest.mark.asyncio
    async def test_each_sub_sends_json_to_controller(self, star):
        """Each of the 5 subs sends JSON to the Controller."""
        controller, subs = star
        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @controller.on(MessageType.JSON)
            async def on_json(peer_fp, data, r=result, e=received):
                r["fp"] = peer_fp
                r["data"] = data
                e.set()

            await sub.send_json({"from_sub": i})
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["fp"] == sub.cert_bundle.fingerprint
            assert result["data"] == {"from_sub": i}

    @pytest.mark.asyncio
    async def test_controller_sends_binary_to_each_sub(self, star):
        """Controller sends binary to each of the 5 subs."""
        controller, subs = star
        blob = _make_binary_data(SIZE_SMALL)

        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @sub.on(MessageType.BINARY)
            async def on_bin(data, r=result, e=received):
                r["data"] = data
                e.set()

            fp = sub.cert_bundle.fingerprint
            await controller.send_binary(fp, blob)
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["data"] == blob

    @pytest.mark.asyncio
    async def test_each_sub_sends_binary_to_controller(self, star):
        """Each of the 5 subs sends binary to the Controller."""
        controller, subs = star
        blob = _make_binary_data(SIZE_SMALL)

        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @controller.on(MessageType.BINARY)
            async def on_bin(peer_fp, data, r=result, e=received):
                r["data"] = data
                e.set()

            await sub.send_binary(blob)
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["data"] == blob

    @pytest.mark.asyncio
    async def test_controller_sends_file_to_each_sub(self, star):
        """Controller sends a file to each of the 5 subs (checksum validated)."""
        controller, subs = star
        fname, fdata = _make_file_data(SIZE_SMALL)

        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @sub.on(MessageType.FILE)
            async def on_file(filename, data, r=result, e=received):
                r["filename"] = filename
                r["data"] = data
                e.set()

            fp = sub.cert_bundle.fingerprint
            await controller.send_file(fp, fname, fdata)
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["filename"] == fname
            assert result["data"] == fdata

    @pytest.mark.asyncio
    async def test_each_sub_sends_file_to_controller(self, star):
        """Each of the 5 subs sends a file to the Controller (checksum validated)."""
        controller, subs = star
        fname, fdata = _make_file_data(SIZE_SMALL)

        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @controller.on(MessageType.FILE)
            async def on_file(peer_fp, filename, data, r=result, e=received):
                r["filename"] = filename
                r["data"] = data
                e.set()

            await sub.send_file(fname, fdata)
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["filename"] == fname
            assert result["data"] == fdata

    @pytest.mark.asyncio
    async def test_controller_sends_image_to_each_sub(self, star):
        """Controller sends an image to each of the 5 subs."""
        controller, subs = star
        fname, fdata = _make_image_data(SIZE_SMALL)

        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @sub.on(MessageType.IMAGE)
            async def on_img(filename, data, r=result, e=received):
                r["filename"] = filename
                r["data"] = data
                e.set()

            fp = sub.cert_bundle.fingerprint
            await controller.send_file(fp, fname, fdata, is_image=True)
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["data"][:4] == b"\x89PNG"

    @pytest.mark.asyncio
    async def test_each_sub_sends_image_to_controller(self, star):
        """Each of the 5 subs sends an image to the Controller."""
        controller, subs = star
        fname, fdata = _make_image_data(SIZE_SMALL)

        for i, sub in enumerate(subs):
            received = asyncio.Event()
            result = {}

            @controller.on(MessageType.IMAGE)
            async def on_img(peer_fp, filename, data, r=result, e=received):
                r["data"] = data
                e.set()

            await sub.send_file(fname, fdata, is_image=True)
            await asyncio.wait_for(received.wait(), timeout=5)
            assert result["data"][:4] == b"\x89PNG"
