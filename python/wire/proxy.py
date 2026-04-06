"""
ReverseProxy — lightweight async HTTP reverse proxy.

Maps URL path prefixes to upstream HTTP services, forwarding requests
and streaming responses back. Useful for exposing multiple Docker
containers (or any HTTP backends) through a single entry point.

Usage:
    proxy = ReverseProxy(host="0.0.0.0", port=8080)
    proxy.add_route("/api", "http://backend:3000")
    proxy.add_route("/dashboard", "http://frontend:8080")
    await proxy.start()
    # ...
    await proxy.stop()
"""

import logging
from typing import Optional

from aiohttp import ClientSession, ClientTimeout, TCPConnector, web

logger = logging.getLogger("wire.proxy")

# Hop-by-hop headers that must NOT be forwarded
_HOP_BY_HOP = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
})


class ReverseProxy:
    """Async HTTP reverse proxy that routes requests by path prefix.

    Supports two kinds of routes:
    - **Direct routes**: prefix → upstream URL (classic reverse proxy)
    - **Tunnel routes**: prefix → HTTP-over-WebSocket tunnel through a Wire peer
    """

    def __init__(self, host: str = "0.0.0.0", port: int = 8080, read_timeout: float = 30.0):
        self.host = host
        self.port = port
        self.read_timeout = read_timeout
        self._routes: dict[str, str] = {}  # path_prefix -> upstream_url
        # Tunnel routes: prefix -> {"peer_fp": str, "tunnel_fn": callable}
        self._tunnel_routes: dict[str, dict] = {}
        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None
        self._session: Optional[ClientSession] = None

    def add_route(self, path_prefix: str, upstream_url: str) -> None:
        """Register a path prefix to forward to an upstream URL.

        The prefix is matched using longest-prefix-first. The matched
        prefix is stripped before forwarding — e.g. a request to
        ``/api/users`` with prefix ``/api`` and upstream
        ``http://backend:3000`` is forwarded to
        ``http://backend:3000/users``.
        """
        # Normalise: ensure prefix starts with / and strip trailing /
        prefix = "/" + path_prefix.strip("/")
        upstream = upstream_url.rstrip("/")
        self._routes[prefix] = upstream
        logger.info("Route added: %s -> %s", prefix, upstream)

    def add_tunnel_route(self, path_prefix: str, peer_fp: str, tunnel_fn) -> None:
        """Register a tunnel route that forwards requests through a WebSocket tunnel.

        Args:
            path_prefix: URL prefix to match.
            peer_fp: Fingerprint of the SubController peer.
            tunnel_fn: Async callable with signature
                ``(peer_fp, method, path, query, headers, body, timeout)
                -> (status, headers, body)``.
        """
        prefix = "/" + path_prefix.strip("/")
        self._tunnel_routes[prefix] = {"peer_fp": peer_fp, "tunnel_fn": tunnel_fn}
        logger.info("Tunnel route added: %s -> peer %s", prefix, peer_fp[:16] + "...")

    def remove_route(self, path_prefix: str) -> None:
        """Remove a previously registered route (direct or tunnel)."""
        prefix = "/" + path_prefix.strip("/")
        self._routes.pop(prefix, None)
        self._tunnel_routes.pop(prefix, None)
        logger.info("Route removed: %s", prefix)

    @property
    def routes(self) -> dict[str, str]:
        """Return a copy of the current route table."""
        return dict(self._routes)

    async def start(self) -> None:
        """Start the HTTP proxy server."""
        self._session = ClientSession(
            connector=TCPConnector(limit=100),
            timeout=ClientTimeout(total=300),
        )
        self._app = web.Application()
        self._app.router.add_route("*", "/{path_info:.*}", self._handle)
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()
        logger.info("ReverseProxy listening on http://%s:%d", self.host, self.port)

    async def stop(self) -> None:
        """Shut down the proxy server and close the HTTP client."""
        if self._session:
            await self._session.close()
            self._session = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        logger.info("ReverseProxy stopped.")

    # -- internal ----------------------------------------------------------

    def _match_route(self, path: str) -> tuple[Optional[str], Optional[str], Optional[dict]]:
        """Find the longest matching prefix for *path*.

        Returns ``(upstream_url_or_none, remainder, tunnel_info_or_none)``.
        ``tunnel_info`` is set for tunnel routes and ``None`` for direct routes.
        """
        best_prefix: Optional[str] = None
        is_tunnel = False

        # Check both direct and tunnel routes for longest match
        for prefix in self._routes:
            if path == prefix or path.startswith(prefix + "/") or prefix == "/":
                if best_prefix is None or len(prefix) > len(best_prefix):
                    best_prefix = prefix
                    is_tunnel = False

        for prefix in self._tunnel_routes:
            if path == prefix or path.startswith(prefix + "/") or prefix == "/":
                if best_prefix is None or len(prefix) > len(best_prefix):
                    best_prefix = prefix
                    is_tunnel = True

        if best_prefix is None:
            return None, None, None

        if best_prefix == "/":
            remainder = path
        else:
            remainder = path[len(best_prefix):]
        if not remainder.startswith("/"):
            remainder = "/" + remainder

        if is_tunnel:
            return None, remainder, self._tunnel_routes[best_prefix]
        else:
            return self._routes[best_prefix], remainder, None

    async def _handle(self, request: web.Request) -> web.StreamResponse:
        """Route an incoming request to the matching upstream."""
        upstream, remainder, tunnel_info = self._match_route(request.path)

        if upstream is None and tunnel_info is None:
            return web.Response(status=404, text="No matching route")

        # ── Tunnel route: forward through WebSocket ──
        if tunnel_info is not None:
            return await self._handle_tunnel(request, remainder, tunnel_info)

        # ── Direct route: forward HTTP directly ──
        target_url = upstream + remainder
        if request.query_string:
            target_url += "?" + request.query_string

        # Build forwarded headers
        headers = self._forward_request_headers(request)

        try:
            body = await request.read()
            async with self._session.request(
                method=request.method,
                url=target_url,
                headers=headers,
                data=body if body else None,
                allow_redirects=False,
            ) as upstream_resp:
                response = web.StreamResponse(
                    status=upstream_resp.status,
                    headers=self._forward_response_headers(upstream_resp),
                )
                await response.prepare(request)

                async for chunk in upstream_resp.content.iter_any():
                    await response.write(chunk)

                await response.write_eof()
                return response

        except Exception as exc:
            logger.error("Upstream error for %s: %s", target_url, exc)
            return web.Response(status=502, text="Bad Gateway")

    async def _handle_tunnel(
        self, request: web.Request, remainder: str, tunnel_info: dict
    ) -> web.Response:
        """Handle a request by tunneling it through a WebSocket to a peer."""
        peer_fp = tunnel_info["peer_fp"]
        tunnel_fn = tunnel_info["tunnel_fn"]

        body = await request.read()
        headers = [
            (k, v)
            for k, v in request.headers.items()
            if k.lower() not in _HOP_BY_HOP and k.lower() != "host"
        ]
        # Add X-Forwarded-* headers
        peer = request.remote or "unknown"
        headers.append(("X-Forwarded-For", peer))
        headers.append(("X-Forwarded-Host", request.host))
        headers.append(("X-Forwarded-Proto", request.scheme))

        # Send the original full path so the SubController can match its services
        status, resp_headers, resp_body = await tunnel_fn(
            peer_fp,
            request.method,
            request.path,
            request.query_string,
            headers,
            body,
            timeout=self.read_timeout,
        )

        response = web.Response(status=status, body=resp_body)
        for k, v in resp_headers:
            response.headers.add(k, v)
        return response

    @staticmethod
    def _forward_request_headers(request: web.Request) -> dict[str, str]:
        """Copy request headers, strip hop-by-hop, add X-Forwarded-*."""
        headers: dict[str, str] = {}
        for key, value in request.headers.items():
            if key.lower() not in _HOP_BY_HOP and key.lower() != "host":
                headers[key] = value

        # X-Forwarded-* headers
        peer = request.remote or "unknown"
        headers["X-Forwarded-For"] = peer
        headers["X-Forwarded-Host"] = request.host
        headers["X-Forwarded-Proto"] = request.scheme

        return headers

    @staticmethod
    def _forward_response_headers(
        upstream_resp,
    ) -> dict[str, str]:
        """Copy response headers, strip hop-by-hop."""
        headers: dict[str, str] = {}
        for key, value in upstream_resp.headers.items():
            if key.lower() not in _HOP_BY_HOP:
                headers[key] = value
        return headers
