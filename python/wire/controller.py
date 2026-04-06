"""
Controller (server) — listens for SubController connections over WSS.

Handles:
  - Self-signed TLS with cert generation on startup
  - Pre-shared secret authentication on first connect
  - Cert fingerprint pinning after initial auth
  - JSON / Binary / File / Image / Streaming over a single WebSocket
  - Relay: forwards messages between SubControllers (star topology)
  - Peer notifications: informs SubControllers of peer join/leave events
"""

import asyncio
import json
import logging
import ssl
import uuid
from typing import Any, Callable, Coroutine, Optional

import websockets
import websockets.asyncio.server

from wire.certs import CertBundle, create_ssl_context_server, generate_self_signed_cert
from wire.protocol import (
    HEADER_SIZE,
    STREAM_CHUNK_SIZE,
    Flags,
    HttpMethod,
    MessageType,
    decode_file_payload,
    decode_frame,
    decode_http_request,
    decode_http_response,
    decode_relay_payload,
    encode_file_payload,
    encode_frame,
    encode_http_request,
    encode_relay_payload,
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
        proxy: "Optional[Any]" = None,
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

        # Health check config
        self._health_check_interval: float = 10.0
        self._health_check_timeout: float = 5.0
        self._unhealthy_threshold: int = 3
        self._healthy_threshold: int = 1
        self._health_check_task: asyncio.Task | None = None
        # Track consecutive health check results
        self._health_check_fails: dict[str, int] = {}
        self._health_check_successes: dict[str, int] = {}

        # Proxy integration
        self._proxy = proxy
        # Tunnel route table: prefix -> {"upstream", "peer_fp", "health_check", "healthy"}
        self._tunnel_routes: dict[str, dict] = {}
        # Pending HTTP tunnel requests: msg_id -> asyncio.Future
        self._pending_requests: dict[bytes, asyncio.Future] = {}

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
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            logger.info("Controller stopped.")

    def configure_health_check(
        self,
        interval: float = 10.0,
        timeout: float = 5.0,
        unhealthy_threshold: int = 3,
        healthy_threshold: int = 1,
    ) -> None:
        """Configure health checking parameters."""
        self._health_check_interval = interval
        self._health_check_timeout = timeout
        self._unhealthy_threshold = unhealthy_threshold
        self._healthy_threshold = healthy_threshold

    def start_health_checks(self) -> None:
        """Start the background health check loop."""
        if self._health_check_task is None:
            self._health_check_task = asyncio.create_task(self._health_check_loop())

    async def _health_check_loop(self):
        """Periodically check health of tunnel routes."""
        try:
            while True:
                await asyncio.sleep(self._health_check_interval)
                for prefix, route in list(self._tunnel_routes.items()):
                    hc_path = route.get("health_check")
                    if not hc_path:
                        continue
                    peer_fp = route["peer_fp"]
                    try:
                        status, _, _ = await self.tunnel_request(
                            peer_fp, "GET", hc_path, "",
                            [], b"", timeout=self._health_check_timeout,
                        )
                        if 200 <= status < 400:
                            # Successful probe
                            self._health_check_fails[prefix] = 0
                            if not route["healthy"]:
                                successes = self._health_check_successes.get(prefix, 0) + 1
                                self._health_check_successes[prefix] = successes
                                if successes >= self._healthy_threshold:
                                    route["healthy"] = True
                                    self._health_check_successes[prefix] = 0
                                    logger.info("Route %s is healthy again", prefix)
                            else:
                                self._health_check_successes[prefix] = 0
                        else:
                            self._record_health_failure(prefix, route)
                    except Exception:
                        self._record_health_failure(prefix, route)
        except asyncio.CancelledError:
            pass

    def _record_health_failure(self, prefix: str, route: dict) -> None:
        self._health_check_successes[prefix] = 0
        fails = self._health_check_fails.get(prefix, 0) + 1
        self._health_check_fails[prefix] = fails
        if fails >= self._unhealthy_threshold and route["healthy"]:
            route["healthy"] = False
            logger.warning(
                "Route %s marked unhealthy after %d failures", prefix, fails
            )

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
        file_payload = encode_file_payload(filename, data)
        await self._send_payload_streamed(ws, msg_type, file_payload)

    async def broadcast_json(self, data: Any):
        """Send JSON to all connected peers."""
        payload = json.dumps(data).encode("utf-8")
        frame = encode_frame(MessageType.JSON, payload, compress=True)
        for ws in self._peers.values():
            await ws.send(frame)

    @property
    def peer_fingerprints(self) -> list[str]:
        return list(self._peers.keys())

    @property
    def tunnel_routes(self) -> dict[str, dict]:
        """Return a copy of the tunnel route table."""
        return dict(self._tunnel_routes)

    def register_peer_routes(self, peer_fp: str, services: list[dict]) -> None:
        """Register tunnel routes from a peer's advertised services."""
        for svc in services:
            prefix = "/" + svc["prefix"].strip("/")
            route = {
                "upstream": svc["upstream"],
                "peer_fp": peer_fp,
                "health_check": svc.get("health_check"),
                "healthy": True,
            }
            self._tunnel_routes[prefix] = route
            logger.info(
                "Tunnel route registered: %s -> peer %s (upstream %s)",
                prefix, peer_fp[:16] + "...", svc["upstream"],
            )
            # Register with proxy if attached
            if self._proxy is not None:
                self._proxy.add_tunnel_route(prefix, peer_fp, self.tunnel_request)

    def deregister_peer_routes(self, peer_fp: str) -> None:
        """Remove all tunnel routes owned by a peer."""
        to_remove = [
            prefix for prefix, route in self._tunnel_routes.items()
            if route["peer_fp"] == peer_fp
        ]
        for prefix in to_remove:
            del self._tunnel_routes[prefix]
            logger.info("Tunnel route removed: %s", prefix)
            if self._proxy is not None:
                self._proxy.remove_route(prefix)

    async def tunnel_request(
        self,
        peer_fp: str,
        method: str,
        path: str,
        query: str,
        headers: list[tuple[str, str]],
        body: bytes,
        timeout: float = 30.0,
    ) -> tuple[int, list[tuple[str, str]], bytes]:
        """Send an HTTP request through the WebSocket tunnel to a peer.

        Returns (status_code, response_headers, response_body).
        """
        ws = self._peers.get(peer_fp)
        if not ws:
            return 502, [], b"Peer not connected"

        http_method = HttpMethod.from_str(method)
        req_payload = encode_http_request(http_method, path, query, headers, body)
        msg_id = uuid.uuid4().bytes

        loop = asyncio.get_event_loop()
        future: asyncio.Future = loop.create_future()
        self._pending_requests[msg_id] = future

        try:
            frame = encode_frame(
                MessageType.HTTP_REQUEST, req_payload, msg_id=msg_id, compress=True
            )
            await ws.send(frame)
            resp_payload = await asyncio.wait_for(future, timeout=timeout)
            return decode_http_response(resp_payload)
        except asyncio.TimeoutError:
            return 504, [], b"Gateway Timeout"
        except Exception as exc:
            logger.error("Tunnel request error: %s", exc)
            return 502, [], b"Bad Gateway"
        finally:
            self._pending_requests.pop(msg_id, None)

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

            # Step 2: Notify existing peers about the new peer, and send
            # the new peer a list of already-connected peers.
            await self._notify_peer_joined(peer_fp)

            # Step 3: Message loop
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
                self.deregister_peer_routes(peer_fp)
                await self._notify_peer_left(peer_fp)

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

        # Register tunnel routes if the peer advertised services
        peer_services = auth_data.get("services", [])
        if peer_services:
            self.register_peer_routes(peer_fp, peer_services)

        return peer_fp

    async def _dispatch(self, peer_fp: str, raw: bytes):
        """Decode a frame and dispatch to the appropriate handler."""
        header, payload = decode_frame(raw)

        # Handle streaming reassembly
        if header.flags & (Flags.STREAM_START | Flags.STREAM_CHUNK | Flags.STREAM_END):
            await self._handle_stream_chunk(peer_fp, header, payload)
            return

        # HTTP_RESPONSE — resolve the pending tunnel request Future
        if header.msg_type == MessageType.HTTP_RESPONSE:
            future = self._pending_requests.get(header.msg_id)
            if future and not future.done():
                future.set_result(payload)
            return

        # Relay messages — forward to destination peer
        if header.msg_type == MessageType.RELAY:
            await self._handle_relay(peer_fp, payload)
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

                # Relay messages need special handling
                if msg_type == MessageType.RELAY:
                    await self._handle_relay(peer_fp, full)
                    return

                handler = self._handlers.get(msg_type)
                if handler:
                    if msg_type in (MessageType.FILE, MessageType.IMAGE):
                        filename, file_data = decode_file_payload(full)
                        await handler(peer_fp, filename, file_data)
                    else:
                        await handler(peer_fp, full)

    async def _handle_relay(self, sender_fp: str, payload: bytes):
        """Decode a relay payload and forward it to the destination peer."""
        source_fp, dest_fp, inner_type, inner_payload = decode_relay_payload(payload)

        ws = self._peers.get(dest_fp)
        if ws is None:
            logger.warning(
                "Relay target not connected: %s (from %s)",
                dest_fp[:16], sender_fp[:16],
            )
            return

        # Re-wrap with the actual sender fingerprint (don't trust client-supplied source)
        relay_out = encode_relay_payload(sender_fp, dest_fp, inner_type, inner_payload)
        await self._send_payload_streamed(ws, MessageType.RELAY, relay_out)

    async def _notify_peer_joined(self, new_fp: str):
        """Tell all existing peers about the new peer, and tell the new
        peer about all already-connected peers."""
        existing_fps = [fp for fp in self._peers if fp != new_fp]

        # Tell the new peer about everyone else
        if existing_fps:
            new_ws = self._peers[new_fp]
            for fp in existing_fps:
                event = json.dumps({"_wire_peer_event": "joined", "peer_fp": fp}).encode()
                frame = encode_frame(MessageType.JSON, event, compress=True)
                await new_ws.send(frame)

        # Tell everyone else about the new peer
        event = json.dumps({"_wire_peer_event": "joined", "peer_fp": new_fp}).encode()
        frame = encode_frame(MessageType.JSON, event, compress=True)
        for fp, ws in self._peers.items():
            if fp != new_fp:
                try:
                    await ws.send(frame)
                except Exception:
                    pass

    async def _notify_peer_left(self, gone_fp: str):
        """Tell all remaining peers that a peer has disconnected."""
        event = json.dumps({"_wire_peer_event": "left", "peer_fp": gone_fp}).encode()
        frame = encode_frame(MessageType.JSON, event, compress=True)
        for ws in self._peers.values():
            try:
                await ws.send(frame)
            except Exception:
                pass

    async def _send_payload_streamed(
        self, ws, msg_type: MessageType, payload: bytes
    ):
        """Send any payload in streaming chunks over the WebSocket."""
        msg_id = uuid.uuid4().bytes

        if len(payload) <= STREAM_CHUNK_SIZE:
            frame = encode_frame(msg_type, payload, msg_id=msg_id, compress=True)
            await ws.send(frame)
            return

        offset = 0
        first = True
        while offset < len(payload):
            chunk = payload[offset : offset + STREAM_CHUNK_SIZE]
            is_last = (offset + STREAM_CHUNK_SIZE) >= len(payload)

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
