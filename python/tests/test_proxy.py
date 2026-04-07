"""Tests for the ReverseProxy HTTP reverse proxy."""

import asyncio
import json

import pytest
import pytest_asyncio
from aiohttp import web

from wire.proxy import ReverseProxy

# ── helpers ──────────────────────────────────────────────────────────────────

PORT_BASE = 21000

def _next_port():
    _next_port.counter += 1
    return PORT_BASE + _next_port.counter

_next_port.counter = 0


def _make_upstream_app():
    """Create a tiny aiohttp app that echoes request info back as JSON."""
    app = web.Application()

    async def echo(request: web.Request) -> web.Response:
        body = await request.read()
        payload = {
            "method": request.method,
            "path": request.path,
            "query": request.query_string,
            "headers": dict(request.headers),
            "body": body.decode("utf-8", errors="replace") if body else "",
        }
        return web.json_response(payload)

    app.router.add_route("*", "/{path_info:.*}", echo)
    return app


@pytest_asyncio.fixture
async def upstream():
    """Start a mock upstream HTTP server and yield (host, port)."""
    port = _next_port()
    app = _make_upstream_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", port)
    await site.start()
    yield "127.0.0.1", port
    await runner.cleanup()


@pytest_asyncio.fixture
async def proxy_with_upstream(upstream):
    """Start a ReverseProxy pointing /api at the upstream, yield (proxy, proxy_port)."""
    host, up_port = upstream
    proxy_port = _next_port()
    proxy = ReverseProxy(host="127.0.0.1", port=proxy_port)
    proxy.add_route("/api", f"http://{host}:{up_port}")
    await proxy.start()
    yield proxy, proxy_port
    await proxy.stop()


# ── route matching ───────────────────────────────────────────────────────────

class TestRouteMatching:
    def test_match_exact_prefix(self):
        proxy = ReverseProxy()
        proxy.add_route("/api", "http://backend:3000")
        upstream, remainder, _ = proxy._match_route("/api")
        assert upstream == "http://backend:3000"
        assert remainder == "/"

    def test_match_subpath(self):
        proxy = ReverseProxy()
        proxy.add_route("/api", "http://backend:3000")
        upstream, remainder, _ = proxy._match_route("/api/users/42")
        assert upstream == "http://backend:3000"
        assert remainder == "/users/42"

    def test_no_match(self):
        proxy = ReverseProxy()
        proxy.add_route("/api", "http://backend:3000")
        upstream, remainder, _ = proxy._match_route("/dashboard")
        assert upstream is None
        assert remainder is None

    def test_longest_prefix_wins(self):
        proxy = ReverseProxy()
        proxy.add_route("/api", "http://general:3000")
        proxy.add_route("/api/v2", "http://v2-backend:3001")
        upstream, remainder, _ = proxy._match_route("/api/v2/items")
        assert upstream == "http://v2-backend:3001"
        assert remainder == "/items"

    def test_root_route_catches_all(self):
        proxy = ReverseProxy()
        proxy.add_route("/", "http://default:80")
        upstream, remainder, _ = proxy._match_route("/anything/here")
        assert upstream == "http://default:80"
        assert remainder == "/anything/here"

    def test_add_and_remove_route(self):
        proxy = ReverseProxy()
        proxy.add_route("/svc", "http://svc:5000")
        assert "/svc" in proxy.routes
        proxy.remove_route("/svc")
        assert "/svc" not in proxy.routes

    def test_trailing_slash_normalised(self):
        proxy = ReverseProxy()
        proxy.add_route("/api/", "http://backend:3000/")
        assert "/api" in proxy.routes
        assert proxy.routes["/api"] == "http://backend:3000"


# ── HTTP forwarding (integration) ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_get_forwarded(proxy_with_upstream):
    """GET /api/hello is forwarded to the upstream as /hello."""
    from aiohttp import ClientSession

    proxy, proxy_port = proxy_with_upstream
    async with ClientSession() as client:
        async with client.get(f"http://127.0.0.1:{proxy_port}/api/hello") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["method"] == "GET"
            assert data["path"] == "/hello"


@pytest.mark.asyncio
async def test_post_with_body(proxy_with_upstream):
    """POST body is forwarded to upstream."""
    from aiohttp import ClientSession

    proxy, proxy_port = proxy_with_upstream
    payload = json.dumps({"key": "value"})
    async with ClientSession() as client:
        async with client.post(
            f"http://127.0.0.1:{proxy_port}/api/data",
            data=payload,
            headers={"Content-Type": "application/json"},
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["method"] == "POST"
            assert data["body"] == payload


@pytest.mark.asyncio
async def test_query_string_forwarded(proxy_with_upstream):
    """Query string parameters are preserved."""
    from aiohttp import ClientSession

    proxy, proxy_port = proxy_with_upstream
    async with ClientSession() as client:
        async with client.get(
            f"http://127.0.0.1:{proxy_port}/api/search?q=hello&page=2"
        ) as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["query"] == "q=hello&page=2"


@pytest.mark.asyncio
async def test_x_forwarded_headers(proxy_with_upstream):
    """X-Forwarded-* headers are injected."""
    from aiohttp import ClientSession

    proxy, proxy_port = proxy_with_upstream
    async with ClientSession() as client:
        async with client.get(f"http://127.0.0.1:{proxy_port}/api/check") as resp:
            assert resp.status == 200
            data = await resp.json()
            headers = data["headers"]
            assert "X-Forwarded-For" in headers
            assert "X-Forwarded-Host" in headers
            assert "X-Forwarded-Proto" in headers


@pytest.mark.asyncio
async def test_404_no_matching_route():
    """Request to an unregistered path returns 404."""
    from aiohttp import ClientSession

    port = _next_port()
    proxy = ReverseProxy(host="127.0.0.1", port=port)
    proxy.add_route("/api", "http://127.0.0.1:1")  # doesn't matter
    await proxy.start()
    try:
        async with ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{port}/unknown/path") as resp:
                assert resp.status == 404
    finally:
        await proxy.stop()


@pytest.mark.asyncio
async def test_502_unreachable_upstream():
    """Request to a dead upstream returns 502."""
    from aiohttp import ClientSession

    port = _next_port()
    proxy = ReverseProxy(host="127.0.0.1", port=port)
    # Point to a port that nothing listens on
    proxy.add_route("/dead", "http://127.0.0.1:1")
    await proxy.start()
    try:
        async with ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{port}/dead/test") as resp:
                assert resp.status == 502
    finally:
        await proxy.stop()


@pytest.mark.asyncio
async def test_multiple_routes(upstream):
    """Multiple routes dispatch to the correct upstream."""
    from aiohttp import ClientSession

    host, up_port = upstream
    proxy_port = _next_port()
    proxy = ReverseProxy(host="127.0.0.1", port=proxy_port)
    proxy.add_route("/svc-a", f"http://{host}:{up_port}")
    proxy.add_route("/svc-b", f"http://{host}:{up_port}")
    await proxy.start()
    try:
        async with ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{proxy_port}/svc-a/foo") as resp:
                data = await resp.json()
                assert data["path"] == "/foo"
            async with client.get(f"http://127.0.0.1:{proxy_port}/svc-b/bar") as resp:
                data = await resp.json()
                assert data["path"] == "/bar"
    finally:
        await proxy.stop()


@pytest.mark.asyncio
async def test_put_and_delete_methods(proxy_with_upstream):
    """PUT and DELETE methods are forwarded correctly."""
    from aiohttp import ClientSession

    proxy, proxy_port = proxy_with_upstream
    async with ClientSession() as client:
        async with client.put(
            f"http://127.0.0.1:{proxy_port}/api/item/1",
            data=b"updated",
        ) as resp:
            data = await resp.json()
            assert data["method"] == "PUT"
            assert data["body"] == "updated"

        async with client.delete(
            f"http://127.0.0.1:{proxy_port}/api/item/1"
        ) as resp:
            data = await resp.json()
            assert data["method"] == "DELETE"
