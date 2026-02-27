"""
SubController (client) — connects to a Controller over WSS.

Mirrors the Controller's capabilities on the client side:
  - Generates its own self-signed cert on startup
  - Authenticates with pre-shared secret
  - Pins the Controller's cert fingerprint
  - Full bidirectional JSON / Binary / File / Image / Stream
"""

import asyncio
import json
import logging
import uuid
from typing import Any, Callable, Coroutine, Optional

import websockets
import websockets.asyncio.client

from wire.certs import CertBundle, create_ssl_context_client, generate_self_signed_cert
from wire.protocol import (
    STREAM_CHUNK_SIZE,
    Flags,
    MessageType,
    decode_file_payload,
    decode_frame,
    encode_file_payload,
    encode_frame,
)

logger = logging.getLogger("wire.subcontroller")

Handler = Callable[..., Coroutine[Any, Any, Any]]


class SubController:
    """WebSocket client node that connects to a Controller."""

    def __init__(
        self,
        controller_url: str = "wss://localhost:8765",
        preshared_secret: str = "",
        cert_dir: str | None = None,
    ):
        self.controller_url = controller_url
        self.preshared_secret = preshared_secret
        self.cert_dir = cert_dir
        self.cert_bundle: CertBundle | None = None

        # The controller's pinned fingerprint (set after first auth)
        self.controller_fingerprint: str | None = None

        self._ws: websockets.asyncio.client.ClientConnection | None = None
        self._handlers: dict[MessageType, Handler] = {}
        self._listen_task: asyncio.Task | None = None

        # Stream reassembly
        self._stream_buffers: dict[bytes, list[bytes]] = {}
        self._stream_meta: dict[bytes, MessageType] = {}

    # -- public API ----------------------------------------------------------

    def on(self, msg_type: MessageType):
        """Decorator to register a handler for a message type."""

        def decorator(fn: Handler):
            self._handlers[msg_type] = fn
            return fn

        return decorator

    async def connect(self):
        """Generate certs, connect to the controller, and authenticate."""
        logger.info("Generating TLS certificate...")
        self.cert_bundle = generate_self_signed_cert(
            common_name="subcontroller", cert_dir=self.cert_dir
        )
        logger.info(
            "SubController fingerprint: %s",
            self.cert_bundle.fingerprint[:16] + "...",
        )

        ssl_ctx = create_ssl_context_client(self.cert_bundle)

        self._ws = await websockets.asyncio.client.connect(
            self.controller_url,
            ssl=ssl_ctx,
            max_size=None,
        )

        await self._authenticate()
        logger.info("Connected and authenticated to controller.")

        # Start background listener
        self._listen_task = asyncio.create_task(self._listen_loop())

    async def disconnect(self):
        """Close the WebSocket connection."""
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            logger.info("Disconnected from controller.")

    async def send_json(self, data: Any):
        """Send a JSON message to the controller."""
        self._ensure_connected()
        payload = json.dumps(data).encode("utf-8")
        frame = encode_frame(MessageType.JSON, payload, compress=True)
        await self._ws.send(frame)

    async def send_binary(self, data: bytes):
        """Send raw binary data to the controller."""
        self._ensure_connected()
        frame = encode_frame(MessageType.BINARY, data, compress=True)
        await self._ws.send(frame)

    async def send_file(self, filename: str, data: bytes, is_image: bool = False):
        """Send a file (or image). Streams in chunks for large files."""
        self._ensure_connected()
        msg_type = MessageType.IMAGE if is_image else MessageType.FILE
        await self._send_streamed(self._ws, msg_type, filename, data)

    # -- internal ------------------------------------------------------------

    def _ensure_connected(self):
        if self._ws is None:
            raise RuntimeError("Not connected. Call connect() first.")

    async def _authenticate(self):
        """Send pre-shared secret + our cert, receive controller's cert."""
        auth_payload = json.dumps({
            "secret": self.preshared_secret,
            "cert_pem": self.cert_bundle.cert_pem.decode("utf-8"),
            "fingerprint": self.cert_bundle.fingerprint,
        }).encode("utf-8")

        frame = encode_frame(MessageType.AUTH, auth_payload)
        await self._ws.send(frame)

        # Wait for AUTH_OK or AUTH_FAIL
        raw = await asyncio.wait_for(self._ws.recv(), timeout=10)
        if isinstance(raw, str):
            raw = raw.encode("utf-8")

        header, payload = decode_frame(raw)

        if header.msg_type == MessageType.AUTH_FAIL:
            error = json.loads(payload).get("error", "unknown")
            raise ConnectionRefusedError(f"Authentication failed: {error}")

        if header.msg_type != MessageType.AUTH_OK:
            raise ConnectionError(
                f"Unexpected response during auth: {header.msg_type}"
            )

        ok_data = json.loads(payload)
        ctrl_fp = ok_data["fingerprint"]

        if self.controller_fingerprint is None:
            # First connection — pin it
            self.controller_fingerprint = ctrl_fp
            logger.info(
                "Pinned controller fingerprint: %s", ctrl_fp[:16] + "..."
            )
        elif self.controller_fingerprint != ctrl_fp:
            raise ConnectionError(
                "Controller fingerprint mismatch! Possible MITM. "
                f"Expected {self.controller_fingerprint[:16]}..., "
                f"got {ctrl_fp[:16]}..."
            )

    async def _listen_loop(self):
        """Background loop that receives and dispatches messages."""
        try:
            async for raw in self._ws:
                if isinstance(raw, str):
                    raw = raw.encode("utf-8")
                try:
                    await self._dispatch(raw)
                except Exception as e:
                    logger.error("Error dispatching message: %s", e)
        except websockets.exceptions.ConnectionClosed:
            logger.info("Connection to controller closed.")
        except asyncio.CancelledError:
            pass

    async def _dispatch(self, raw: bytes):
        """Decode and dispatch a received frame."""
        header, payload = decode_frame(raw)

        # Stream reassembly
        if header.flags & (Flags.STREAM_START | Flags.STREAM_CHUNK | Flags.STREAM_END):
            await self._handle_stream_chunk(header, payload)
            return

        handler = self._handlers.get(header.msg_type)
        if handler is None:
            logger.debug("No handler for message type %s", header.msg_type)
            return

        if header.msg_type == MessageType.JSON:
            data = json.loads(payload)
            await handler(data)
        elif header.msg_type in (MessageType.FILE, MessageType.IMAGE):
            filename, file_data = decode_file_payload(payload)
            await handler(filename, file_data)
        elif header.msg_type == MessageType.BINARY:
            await handler(payload)
        else:
            await handler(payload)

    async def _handle_stream_chunk(self, header, payload: bytes):
        """Reassemble streamed transfers."""
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
                        await handler(filename, file_data)
                    else:
                        await handler(full)

    async def _send_streamed(self, ws, msg_type: MessageType, filename: str, data: bytes):
        """Send a file in streaming chunks."""
        msg_id = uuid.uuid4().bytes
        file_payload = encode_file_payload(filename, data)

        if len(file_payload) <= STREAM_CHUNK_SIZE:
            frame = encode_frame(msg_type, file_payload, msg_id=msg_id, compress=True)
            await ws.send(frame)
            return

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
