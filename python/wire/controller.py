"""
Controller (server) — listens for SubController connections over WSS.

Handles:
  - Self-signed TLS with cert generation on startup
  - Pre-shared secret authentication on first connect
  - Cert fingerprint pinning after initial auth
  - JSON / Binary / File / Image / Streaming over a single WebSocket
"""

import asyncio
import json
import logging
import ssl
from typing import Any, Callable, Coroutine, Optional

import websockets
import websockets.asyncio.server

from wire.certs import CertBundle, create_ssl_context_server, generate_self_signed_cert
from wire.protocol import (
    HEADER_SIZE,
    STREAM_CHUNK_SIZE,
    Flags,
    MessageType,
    decode_file_payload,
    decode_frame,
    encode_file_payload,
    encode_frame,
)

logger = logging.getLogger("wire.controller")

# Type alias for message handlers
Handler = Callable[..., Coroutine[Any, Any, Any]]


class Controller:
    """WebSocket server node that SubControllers connect to."""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8765,
        preshared_secret: str = "",
        cert_dir: str | None = None,
    ):
        self.host = host
        self.port = port
        self.preshared_secret = preshared_secret
        self.cert_bundle: CertBundle | None = None
        self.cert_dir = cert_dir

        # Pinned peer fingerprints -> True once authenticated
        self._pinned_peers: dict[str, bool] = {}
        # Active connections keyed by peer fingerprint
        self._peers: dict[str, websockets.asyncio.server.ServerConnection] = {}

        # User-registered handlers
        self._handlers: dict[MessageType, Handler] = {}

        # Stream reassembly buffers: msg_id -> list of chunk bytes
        self._stream_buffers: dict[bytes, list[bytes]] = {}
        self._stream_meta: dict[bytes, MessageType] = {}

        self._server: Any = None
        self._serve_task: asyncio.Task | None = None

    # -- public API ----------------------------------------------------------

    def on(self, msg_type: MessageType):
        """Decorator to register a handler for a message type."""

        def decorator(fn: Handler):
            self._handlers[msg_type] = fn
            return fn

        return decorator

    async def start(self):
        """Generate certs and start listening."""
        logger.info("Generating TLS certificate...")
        self.cert_bundle = generate_self_signed_cert(
            common_name="controller", cert_dir=self.cert_dir
        )
        logger.info(
            "Controller fingerprint: %s", self.cert_bundle.fingerprint[:16] + "..."
        )

        ssl_ctx = create_ssl_context_server(self.cert_bundle)
        self._server = await websockets.asyncio.server.serve(
            self._handle_connection,
            self.host,
            self.port,
            ssl=ssl_ctx,
            max_size=None,  # no limit, we do our own framing
        )
        logger.info("Controller listening on wss://%s:%d", self.host, self.port)

    async def stop(self):
        """Shut down the server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Controller stopped.")

    async def send_json(self, peer_fp: str, data: Any):
        """Send a JSON message to a connected peer."""
        ws = self._peers.get(peer_fp)
        if not ws:
            raise ValueError(f"No peer with fingerprint {peer_fp[:16]}...")
        payload = json.dumps(data).encode("utf-8")
        frame = encode_frame(MessageType.JSON, payload, compress=True)
        await ws.send(frame)

    async def send_binary(self, peer_fp: str, data: bytes):
        """Send raw binary data to a connected peer."""
        ws = self._peers.get(peer_fp)
        if not ws:
            raise ValueError(f"No peer with fingerprint {peer_fp[:16]}...")
        frame = encode_frame(MessageType.BINARY, data, compress=True)
        await ws.send(frame)

    async def send_file(
        self, peer_fp: str, filename: str, data: bytes, is_image: bool = False
    ):
        """Send a file (or image). Streams in chunks for large files."""
        ws = self._peers.get(peer_fp)
        if not ws:
            raise ValueError(f"No peer with fingerprint {peer_fp[:16]}...")
        msg_type = MessageType.IMAGE if is_image else MessageType.FILE
        await self._send_streamed(ws, msg_type, filename, data)

    async def broadcast_json(self, data: Any):
        """Send JSON to all connected peers."""
        payload = json.dumps(data).encode("utf-8")
        frame = encode_frame(MessageType.JSON, payload, compress=True)
        for ws in self._peers.values():
            await ws.send(frame)

    @property
    def peer_fingerprints(self) -> list[str]:
        return list(self._peers.keys())

    # -- internal ------------------------------------------------------------

    async def _handle_connection(self, ws: websockets.asyncio.server.ServerConnection):
        """Handle a new incoming WebSocket connection."""
        peer_fp = None
        try:
            # Step 1: Authentication handshake
            peer_fp = await self._authenticate(ws)
            if peer_fp is None:
                return

            self._peers[peer_fp] = ws
            logger.info("Peer authenticated: %s", peer_fp[:16] + "...")

            # Step 2: Message loop
            async for raw in ws:
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                try:
                    await self._dispatch(peer_fp, raw)
                except Exception as e:
                    logger.error("Error dispatching message: %s", e)

        except websockets.exceptions.ConnectionClosed:
            logger.info("Peer disconnected: %s", (peer_fp or "unknown")[:16])
        finally:
            if peer_fp and peer_fp in self._peers:
                del self._peers[peer_fp]

    async def _authenticate(self, ws) -> str | None:
        """Perform pre-shared secret + cert fingerprint exchange."""
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=10)
        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
            logger.warning("Auth timeout or connection lost")
            return None

        if isinstance(raw, str):
            raw = raw.encode("utf-8")

        header, payload = decode_frame(raw)
        if header.msg_type != MessageType.AUTH:
            logger.warning("Expected AUTH frame, got %s", header.msg_type)
            await ws.close()
            return None

        auth_data = json.loads(payload)
        peer_secret = auth_data.get("secret", "")
        peer_cert_pem = auth_data.get("cert_pem", "").encode("utf-8")
        peer_fp = auth_data.get("fingerprint", "")

        if peer_secret != self.preshared_secret:
            logger.warning("Auth failed: bad secret")
            fail = encode_frame(
                MessageType.AUTH_FAIL,
                json.dumps({"error": "bad secret"}).encode(),
            )
            await ws.send(fail)
            await ws.close()
            return None

        # Pin the peer's fingerprint on first contact
        if peer_fp in self._pinned_peers:
            logger.info("Known peer reconnecting: %s", peer_fp[:16] + "...")
        else:
            self._pinned_peers[peer_fp] = True
            logger.info("Pinning new peer: %s", peer_fp[:16] + "...")

        # Send back our cert + OK
        ok_payload = json.dumps({
            "cert_pem": self.cert_bundle.cert_pem.decode("utf-8"),
            "fingerprint": self.cert_bundle.fingerprint,
        }).encode("utf-8")
        ok_frame = encode_frame(MessageType.AUTH_OK, ok_payload)
        await ws.send(ok_frame)
        return peer_fp

    async def _dispatch(self, peer_fp: str, raw: bytes):
        """Decode a frame and dispatch to the appropriate handler."""
        header, payload = decode_frame(raw)

        # Handle streaming reassembly
        if header.flags & (Flags.STREAM_START | Flags.STREAM_CHUNK | Flags.STREAM_END):
            await self._handle_stream_chunk(peer_fp, header, payload)
            return

        # Non-streamed messages
        handler = self._handlers.get(header.msg_type)
        if handler is None:
            logger.debug("No handler for message type %s", header.msg_type)
            return

        if header.msg_type == MessageType.JSON:
            data = json.loads(payload)
            await handler(peer_fp, data)
        elif header.msg_type in (MessageType.FILE, MessageType.IMAGE):
            filename, file_data = decode_file_payload(payload)
            await handler(peer_fp, filename, file_data)
        elif header.msg_type == MessageType.BINARY:
            await handler(peer_fp, payload)
        else:
            await handler(peer_fp, payload)

    async def _handle_stream_chunk(
        self, peer_fp: str, header, payload: bytes
    ):
        """Reassemble streamed file/binary transfers."""
        mid = header.msg_id
        if header.flags & Flags.STREAM_START:
            self._stream_buffers[mid] = [payload]
            self._stream_meta[mid] = header.msg_type
        elif header.flags & Flags.STREAM_CHUNK:
            if mid in self._stream_buffers:
                self._stream_buffers[mid].append(payload)
        if header.flags & Flags.STREAM_END:
            if mid in self._stream_buffers:
                self._stream_buffers[mid].append(payload)
                full = b"".join(self._stream_buffers.pop(mid))
                msg_type = self._stream_meta.pop(mid)
                handler = self._handlers.get(msg_type)
                if handler:
                    if msg_type in (MessageType.FILE, MessageType.IMAGE):
                        filename, file_data = decode_file_payload(full)
                        await handler(peer_fp, filename, file_data)
                    else:
                        await handler(peer_fp, full)

    async def _send_streamed(
        self, ws, msg_type: MessageType, filename: str, data: bytes
    ):
        """Send a file in streaming chunks over the WebSocket."""
        import uuid

        msg_id = uuid.uuid4().bytes
        file_payload = encode_file_payload(filename, data)

        if len(file_payload) <= STREAM_CHUNK_SIZE:
            # Small enough for a single frame
            frame = encode_frame(msg_type, file_payload, msg_id=msg_id, compress=True)
            await ws.send(frame)
            return

        # Stream in chunks
        offset = 0
        first = True
        while offset < len(file_payload):
            chunk = file_payload[offset : offset + STREAM_CHUNK_SIZE]
            is_last = (offset + STREAM_CHUNK_SIZE) >= len(file_payload)

            if first:
                flags = Flags.STREAM_START
                first = False
            elif is_last:
                flags = Flags.STREAM_END
            else:
                flags = Flags.STREAM_CHUNK

            frame = encode_frame(msg_type, chunk, msg_id=msg_id, flags=flags)
            await ws.send(frame)
            offset += STREAM_CHUNK_SIZE
