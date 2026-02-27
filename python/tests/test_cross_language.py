"""
Cross-language integration tests — verify that Python and Rust
implementations can interoperate over the shared wire protocol.

Test matrix:
  1. Rust Controller  + Python SubController  (JSON, binary, file)
  2. Python Controller + Rust SubController   (JSON received from Rust)
"""

import asyncio
import io
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import zipfile

import pytest
import pytest_asyncio

from wire.controller import Controller
from wire.subcontroller import SubController
from wire.protocol import MessageType

RUST_BINARY = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "rust",
    "wire-rs",
    "target",
    "release",
    "wire_rs",
)

SECRET = "cross-lang-test-secret-99"


def _rust_binary_path() -> str:
    path = os.path.normpath(RUST_BINARY)
    if not os.path.isfile(path):
        pytest.skip(f"Rust binary not found at {path}; run `cargo build --release` first")
    return path


def _wait_for_port(port: int, timeout: float = 10.0):
    """Block until something is listening on the given port."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.1)
    raise TimeoutError(f"Nothing listening on port {port} after {timeout}s")


# ---------------------------------------------------------------------------
# 1. Rust Controller + Python SubController
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rust_controller_python_sub_json():
    """Python SubController sends JSON to a Rust Controller."""
    port = 21001
    binary = _rust_binary_path()

    proc = subprocess.Popen(
        [binary, "controller", "--host", "127.0.0.1", "--port", str(port), "--secret", SECRET],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        # Wait for the Rust controller to start listening
        _wait_for_port(port)

        sub_dir = tempfile.mkdtemp(prefix="wire_cross_sub_")
        sub = SubController(
            controller_url=f"wss://127.0.0.1:{port}",
            preshared_secret=SECRET,
            cert_dir=sub_dir,
        )

        await sub.connect()
        assert sub.controller_fingerprint is not None

        # Send a JSON message
        await sub.send_json({"from": "python", "value": 42})
        # Give the Rust side time to print it
        await asyncio.sleep(0.5)

        await sub.disconnect()
    finally:
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=5)

    # The Rust controller prints: [JSON from <fp>...]: {"from":"python","value":42}
    output = stdout.decode("utf-8", errors="replace")
    assert '"from"' in output or '"python"' in output, (
        f"Rust controller did not show JSON from Python sub.\nstdout: {output}\nstderr: {stderr.decode()}"
    )


@pytest.mark.asyncio
async def test_rust_controller_python_sub_binary():
    """Python SubController sends binary data to a Rust Controller."""
    port = 21002
    binary = _rust_binary_path()

    proc = subprocess.Popen(
        [binary, "controller", "--host", "127.0.0.1", "--port", str(port), "--secret", SECRET],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_port(port)

        sub_dir = tempfile.mkdtemp(prefix="wire_cross_sub_")
        sub = SubController(
            controller_url=f"wss://127.0.0.1:{port}",
            preshared_secret=SECRET,
            cert_dir=sub_dir,
        )

        await sub.connect()

        blob = bytes(range(256)) * 100  # 25.6 KB
        await sub.send_binary(blob)
        await asyncio.sleep(0.5)

        await sub.disconnect()
    finally:
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=5)

    output = stdout.decode("utf-8", errors="replace")
    assert "25600 bytes" in output, (
        f"Rust controller did not show binary from Python sub.\nstdout: {output}\nstderr: {stderr.decode()}"
    )


@pytest.mark.asyncio
async def test_rust_controller_python_sub_file():
    """Python SubController sends a zip file to a Rust Controller."""
    port = 21003
    binary = _rust_binary_path()

    proc = subprocess.Popen(
        [binary, "controller", "--host", "127.0.0.1", "--port", str(port), "--secret", SECRET],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_for_port(port)

        sub_dir = tempfile.mkdtemp(prefix="wire_cross_sub_")
        sub = SubController(
            controller_url=f"wss://127.0.0.1:{port}",
            preshared_secret=SECRET,
            cert_dir=sub_dir,
        )

        await sub.connect()

        # Create a real zip
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("hello.txt", "Hello from Python!")
        zip_bytes = buf.getvalue()

        await sub.send_file("cross_test.zip", zip_bytes)
        await asyncio.sleep(0.5)

        await sub.disconnect()
    finally:
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=5)

    output = stdout.decode("utf-8", errors="replace")
    assert "cross_test.zip" in output, (
        f"Rust controller did not show file from Python sub.\nstdout: {output}\nstderr: {stderr.decode()}"
    )


# ---------------------------------------------------------------------------
# 2. Python Controller + Rust SubController
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_python_controller_rust_sub_json():
    """Rust SubController sends JSON to a Python Controller on connect."""
    port = 21004
    binary = _rust_binary_path()

    ctrl_dir = tempfile.mkdtemp(prefix="wire_cross_ctrl_")
    controller = Controller(
        host="127.0.0.1",
        port=port,
        preshared_secret=SECRET,
        cert_dir=ctrl_dir,
    )
    await controller.start()

    received = asyncio.Event()
    result = {}

    @controller.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        result.update(data)
        received.set()

    try:
        # Launch Rust sub — it sends {"hello": "from rust subcontroller"} on connect
        proc = subprocess.Popen(
            [binary, "sub", "--host", "127.0.0.1", "--port", str(port), "--secret", SECRET],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        try:
            await asyncio.wait_for(received.wait(), timeout=10)
            assert result == {"hello": "from rust subcontroller"}
        finally:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=5)
    finally:
        await controller.stop()


@pytest.mark.asyncio
async def test_python_controller_sends_json_to_rust_sub():
    """Python Controller sends JSON, Rust SubController receives and prints it."""
    port = 21005
    binary = _rust_binary_path()

    ctrl_dir = tempfile.mkdtemp(prefix="wire_cross_ctrl_")
    controller = Controller(
        host="127.0.0.1",
        port=port,
        preshared_secret=SECRET,
        cert_dir=ctrl_dir,
    )

    # Wait for the Rust sub to connect before sending
    connected = asyncio.Event()

    @controller.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        # Rust sub sends {"hello": "from rust subcontroller"} on connect
        connected.set()

    await controller.start()

    proc = subprocess.Popen(
        [binary, "sub", "--host", "127.0.0.1", "--port", str(port), "--secret", SECRET],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        await asyncio.wait_for(connected.wait(), timeout=10)

        # Now send JSON from Python Controller to Rust Sub
        peer_fp = controller.peer_fingerprints[0]
        await controller.send_json(peer_fp, {"from": "python-ctrl", "number": 7})
        await asyncio.sleep(0.5)
    finally:
        proc.send_signal(signal.SIGTERM)
        stdout, stderr = proc.communicate(timeout=5)
        await controller.stop()

    output = stdout.decode("utf-8", errors="replace")
    assert "python-ctrl" in output, (
        f"Rust sub did not show JSON from Python controller.\nstdout: {output}\nstderr: {stderr.decode()}"
    )
