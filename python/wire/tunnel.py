"""
ProxyTunnel — HTTP tunnel through the Wire mesh.

Allows a node (Controller or SubController) to accept HTTP requests
locally and forward them through the Wire protocol to a remote peer,
which makes the actual upstream HTTP call and returns the response.

Traffic flow::

    HTTP client
        |
        v
    Node A  (local HTTP listener, :8080/api)
        |  <-- Wire relay / direct JSON -->
        v
    Node B  (forward proxy → upstream)
        |
        v
    http://backend:3000/api/...

Node A acts as a *reverse proxy* (accepts HTTP from clients).
Node B acts as a *forward proxy* (makes HTTP calls to the upstream).

This module provides:
  - ``ProxyTunnel``: the local HTTP listener on the initiating node.
  - ``handle_tunnel_request``: the handler for the target node that
    makes the upstream HTTP call and sends the response back.

Both are wired in automatically by Controller and SubController —
user code only needs to call ``open_proxy_tunnel()``.
"""

import asyncio
import base64
import logging
import uuid
from typing import Any, Callable, Coroutine

from aiohttp import ClientSession, ClientTimeout, TCPConnector, web

logger = logging.getLogger("wire.tunnel")

# Type: async def send(target_fp: str, data: dict) -> None
SendFn = Callable[[str, Any], Coroutine[Any, Any, None]]

_HOP_BY_HOP = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
})


class ProxyTunnel:
    """Local HTTP listener that tunnels requests through Wire to a remote peer.

    Every incoming HTTP request is serialised into a JSON ``_wire_tunnel_req``
    message, sent to ``target_fp`` via ``send_fn``, and the corresponding
    ``_wire_tunnel_res`` is awaited and returned to the HTTP client.
    """

    def __init__(
        self,
        send_fn: SendFn,
        listen_host: str,
        listen_port: int,
        path_prefix: str,
        target_fp: str,
        upstream_url: str,
        timeout: float = 30.0,
    ):
        self.send_fn = send_fn
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.path_prefix = path_prefix.rstrip("/") if path_prefix != "/" else ""
        self.target_fp = target_fp
        self.upstream_url = upstream_url.rstrip("/")
        self.timeout = timeout

        self._pending: dict[str, asyncio.Future] = {}
        self._runner: web.AppRunner | None = None

    async def start(self):
        """Start the local HTTP server that accepts client requests."""
        app = web.Application()
        app.router.add_route("*", "/{path_info:.*}", self._handle)

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.listen_host, self.listen_port)
        await site.start()
        logger.info(
            "Proxy tunnel http://%s:%d%s -> peer %s -> %s",
            self.listen_host,
            self.listen_port,
            self.path_prefix or "/",
            self.target_fp[:16] + "...",
            self.upstream_url,
        )

    async def stop(self):
        """Stop the HTTP listener and cancel any pending requests."""
        for future in self._pending.values():
            if not future.done():
                future.cancel()
        self._pending.clear()
        if self._runner:
            await self._runner.cleanup()
            self._runner = None

    def receive_response(self, data: dict) -> bool:
        """Deliver a ``_wire_tunnel_res`` to the matching pending request.

        Returns True if the response was matched to a pending request.
        """
        req_id = data.get("id", "")
        future = self._pending.pop(req_id, None)
        if future and not future.done():
            future.set_result(data)
            return True
        return False

    # -- internal ----------------------------------------------------------

    async def _handle(self, request: web.Request) -> web.Response:
        """Handle an incoming HTTP request: serialise, tunnel, return response."""
        path = request.path

        # Match path prefix
        if self.path_prefix:
            if not path.startswith(self.path_prefix):
                return web.Response(status=404, text="No matching tunnel route")
            path = path[len(self.path_prefix):]
        if not path.startswith("/"):
            path = "/" + path
        if request.query_string:
            path = f"{path}?{request.query_string}"

        body = await request.read()

        # Filter hop-by-hop headers
        headers = {
            k: v
            for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP
        }

        req_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[req_id] = future

        try:
            # Send tunnelled request through Wire mesh
            await self.send_fn(self.target_fp, {
                "_wire_tunnel_req": {
                    "id": req_id,
                    "method": request.method,
                    "path": path,
                    "headers": headers,
                    "body_b64": base64.b64encode(body).decode() if body else "",
                    "upstream_url": self.upstream_url,
                }
            })

            # Wait for the target peer to respond
            resp_data = await asyncio.wait_for(future, timeout=self.timeout)

            status = resp_data.get("status", 502)
            resp_headers = resp_data.get("headers", {})
            body_b64 = resp_data.get("body_b64", "")
            resp_body = base64.b64decode(body_b64) if body_b64 else b""

            # Filter response headers
            filtered = {
                k: v
                for k, v in resp_headers.items()
                if k.lower() not in _HOP_BY_HOP
                and k.lower() != "content-length"
            }

            return web.Response(status=status, headers=filtered, body=resp_body)

        except asyncio.TimeoutError:
            self._pending.pop(req_id, None)
            return web.Response(status=504, text="Tunnel timeout")
        except Exception as e:
            self._pending.pop(req_id, None)
            logger.error("Tunnel error: %s", e)
            return web.Response(status=502, text=str(e))


async def handle_tunnel_request(
    req_data: dict,
    send_fn: SendFn,
    response_target_fp: str,
) -> None:
    """Execute an incoming ``_wire_tunnel_req``.

    Makes the actual HTTP call to the upstream URL and sends back a
    ``_wire_tunnel_res`` to the originating node.  Called automatically
    by Controller and SubController when they receive tunnel requests.
    """
    req_id = req_data.get("id", "")
    method = req_data.get("method", "GET")
    path = req_data.get("path", "/")
    headers = req_data.get("headers", {})
    body_b64 = req_data.get("body_b64", "")
    upstream_url = req_data.get("upstream_url", "")

    body = base64.b64decode(body_b64) if body_b64 else None
    url = upstream_url.rstrip("/") + path

    try:
        connector = TCPConnector(ssl=False)
        async with ClientSession(
            connector=connector,
            timeout=ClientTimeout(total=30),
        ) as session:
            async with session.request(
                method,
                url,
                headers=headers,
                data=body,
            ) as resp:
                resp_body = await resp.read()
                resp_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() not in _HOP_BY_HOP
                }

                await send_fn(response_target_fp, {
                    "_wire_tunnel_res": {
                        "id": req_id,
                        "status": resp.status,
                        "headers": resp_headers,
                        "body_b64": base64.b64encode(resp_body).decode(),
                    }
                })
    except Exception as e:
        logger.error("Tunnel upstream error (%s): %s", url, e)
        try:
            await send_fn(response_target_fp, {
                "_wire_tunnel_res": {
                    "id": req_id,
                    "status": 502,
                    "headers": {"Content-Type": "text/plain"},
                    "body_b64": base64.b64encode(
                        f"Upstream error: {e}".encode()
                    ).decode(),
                }
            })
        except Exception:
            pass
