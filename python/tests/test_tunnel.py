"""
Integration tests for the HTTP-over-WebSocket tunnel.

Spins up Controller + SubController with advertised services and an
upstream HTTP server, then verifies that HTTP requests are correctly
tunneled through the WebSocket connection.
"""

import asyncio
import json
import tempfile

import pytest
import pytest_asyncio
from aiohttp import web, ClientSession

from wire.controller import Controller
from wire.subcontroller import SubController
from wire.proxy import ReverseProxy
from wire.protocol import (
    HttpMethod,
    encode_http_request,
    decode_http_request,
    encode_http_response,
    decode_http_response,
)

# ── Protocol unit tests ──────────────────────────────────────────────────────

class TestHttpProtocol:
    def test_http_request_roundtrip(self):
        method = HttpMethod.POST
        path = "/api/users"
        query = "page=1&limit=10"
        headers = [("Content-Type", "application/json"), ("X-Custom", "value")]
        body = b'{"name": "test"}'

        encoded = encode_http_request(method, path, query, headers, body)
        dec_method, dec_path, dec_query, dec_headers, dec_body = decode_http_request(encoded)

        assert dec_method == HttpMethod.POST
        assert dec_path == "/api/users"
        assert dec_query == "page=1&limit=10"
        assert dec_headers == headers
        assert dec_body == body

    def test_http_response_roundtrip(self):
        status = 200
        headers = [("Content-Type", "application/json"), ("X-Request-Id", "abc")]
        body = b'{"ok": true}'

        encoded = encode_http_response(status, headers, body)
        dec_status, dec_headers, dec_body = decode_http_response(encoded)

        assert dec_status == 200
        assert dec_headers == headers
        assert dec_body == body

    def test_empty_body(self):
        encoded = encode_http_request(HttpMethod.GET, "/", "", [], b"")
        method, path, query, headers, body = decode_http_request(encoded)
        assert method == HttpMethod.GET
        assert body == b""

    def test_empty_response(self):
        encoded = encode_http_response(204, [], b"")
        status, headers, body = decode_http_response(encoded)
        assert status == 204
        assert body == b""

    def test_all_methods(self):
        for m in HttpMethod:
            encoded = encode_http_request(m, "/test", "", [], b"")
            dec_m, _, _, _, _ = decode_http_request(encoded)
            assert dec_m == m

    def test_method_from_str(self):
        assert HttpMethod.from_str("GET") == HttpMethod.GET
        assert HttpMethod.from_str("post") == HttpMethod.POST
        assert HttpMethod.from_str("Delete") == HttpMethod.DELETE

    def test_method_to_str(self):
        assert HttpMethod.GET.to_str() == "GET"
        assert HttpMethod.POST.to_str() == "POST"


# ── Helpers ──────────────────────────────────────────────────────────────────

PORT_BASE = 23000
_port_counter = 0

def _next_port():
    global _port_counter
    _port_counter += 1
    return PORT_BASE + _port_counter

SECRET = "tunnel-test-secret"


def _make_echo_app():
    """Create a tiny echo HTTP server."""
    app = web.Application()

    async def echo(request: web.Request) -> web.Response:
        body = await request.read()
        payload = {
            "method": request.method,
            "path": request.path,
            "query": request.query_string,
            "body": body.decode("utf-8", errors="replace") if body else "",
        }
        return web.json_response(payload)

    app.router.add_route("*", "/{path_info:.*}", echo)
    return app


# ── Integration tests ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_tunnel_get():
    """GET request tunneled through WebSocket to a SubController's local service."""
    ctrl_port = _next_port()
    upstream_port = _next_port()
    proxy_port = _next_port()
    ctrl_dir = tempfile.mkdtemp()
    sub_dir = tempfile.mkdtemp()

    # Start upstream echo server
    upstream_app = _make_echo_app()
    runner = web.AppRunner(upstream_app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", upstream_port)
    await site.start()

    # Start proxy
    proxy = ReverseProxy(host="127.0.0.1", port=proxy_port)
    await proxy.start()

    # Start controller with proxy
    controller = Controller(
        host="127.0.0.1", port=ctrl_port,
        preshared_secret=SECRET, cert_dir=ctrl_dir,
        proxy=proxy,
    )
    await controller.start()

    # Start sub with services
    sub = SubController(
        controller_url=f"wss://127.0.0.1:{ctrl_port}",
        preshared_secret=SECRET,
        cert_dir=sub_dir,
        services=[{"prefix": "/api", "upstream": f"http://127.0.0.1:{upstream_port}"}],
    )
    await sub.connect()
    await asyncio.sleep(0.2)

    # Verify tunnel route was registered
    assert "/api" in controller.tunnel_routes

    # Make HTTP request through the tunnel via the proxy
    async with ClientSession() as client:
        async with client.get(f"http://127.0.0.1:{proxy_port}/api/hello") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["method"] == "GET"
            assert data["path"] == "/hello"

    # Cleanup
    await sub.disconnect()
    await controller.stop()
    await proxy.stop()
    await runner.cleanup()


@pytest.mark.asyncio
async def test_tunnel_post_with_body():
    """POST request with body tunneled through WebSocket."""
    ctrl_port = _next_port()
    upstream_port = _next_port()
    proxy_port = _next_port()
    ctrl_dir = tempfile.mkdtemp()
    sub_dir = tempfile.mkdtemp()

    upstream_app = _make_echo_app()
    runner = web.AppRunner(upstream_app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", upstream_port)
    await site.start()

    proxy = ReverseProxy(host="127.0.0.1", port=proxy_port)
    await proxy.start()

    controller = Controller(
        host="127.0.0.1", port=ctrl_port,
        preshared_secret=SECRET, cert_dir=ctrl_dir,
        proxy=proxy,
    )
    await controller.start()

    sub = SubController(
        controller_url=f"wss://127.0.0.1:{ctrl_port}",
        preshared_secret=SECRET,
        cert_dir=sub_dir,
        services=[{"prefix": "/api", "upstream": f"http://127.0.0.1:{upstream_port}"}],
    )
    await sub.connect()
    await asyncio.sleep(0.2)

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

    await sub.disconnect()
    await controller.stop()
    await proxy.stop()
    await runner.cleanup()


@pytest.mark.asyncio
async def test_tunnel_route_cleanup_on_disconnect():
    """Routes are cleaned up when a SubController disconnects."""
    ctrl_port = _next_port()
    ctrl_dir = tempfile.mkdtemp()
    sub_dir = tempfile.mkdtemp()

    controller = Controller(
        host="127.0.0.1", port=ctrl_port,
        preshared_secret=SECRET, cert_dir=ctrl_dir,
    )
    await controller.start()

    sub = SubController(
        controller_url=f"wss://127.0.0.1:{ctrl_port}",
        preshared_secret=SECRET,
        cert_dir=sub_dir,
        services=[{"prefix": "/svc", "upstream": "http://localhost:9999"}],
    )
    await sub.connect()
    await asyncio.sleep(0.1)

    assert "/svc" in controller.tunnel_routes

    await sub.disconnect()
    await asyncio.sleep(0.2)

    assert "/svc" not in controller.tunnel_routes

    await controller.stop()


@pytest.mark.asyncio
async def test_tunnel_no_services_backward_compat():
    """SubController without services still works (backward compatibility)."""
    ctrl_port = _next_port()
    ctrl_dir = tempfile.mkdtemp()
    sub_dir = tempfile.mkdtemp()

    controller = Controller(
        host="127.0.0.1", port=ctrl_port,
        preshared_secret=SECRET, cert_dir=ctrl_dir,
    )
    await controller.start()

    sub = SubController(
        controller_url=f"wss://127.0.0.1:{ctrl_port}",
        preshared_secret=SECRET,
        cert_dir=sub_dir,
    )
    await sub.connect()
    await asyncio.sleep(0.1)

    assert len(controller.tunnel_routes) == 0
    assert len(controller.peer_fingerprints) == 1

    await sub.disconnect()
    await controller.stop()


@pytest.mark.asyncio
async def test_tunnel_direct_request():
    """Test tunnel_request directly without proxy."""
    ctrl_port = _next_port()
    upstream_port = _next_port()
    ctrl_dir = tempfile.mkdtemp()
    sub_dir = tempfile.mkdtemp()

    upstream_app = _make_echo_app()
    runner = web.AppRunner(upstream_app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", upstream_port)
    await site.start()

    controller = Controller(
        host="127.0.0.1", port=ctrl_port,
        preshared_secret=SECRET, cert_dir=ctrl_dir,
    )
    await controller.start()

    sub = SubController(
        controller_url=f"wss://127.0.0.1:{ctrl_port}",
        preshared_secret=SECRET,
        cert_dir=sub_dir,
        services=[{"prefix": "/api", "upstream": f"http://127.0.0.1:{upstream_port}"}],
    )
    await sub.connect()
    await asyncio.sleep(0.2)

    peer_fp = controller.peer_fingerprints[0]
    status, headers, body = await controller.tunnel_request(
        peer_fp, "GET", "/api/test", "q=1", [], b"",
    )
    assert status == 200
    data = json.loads(body)
    assert data["method"] == "GET"
    assert data["path"] == "/test"
    assert data["query"] == "q=1"

    await sub.disconnect()
    await controller.stop()
    await runner.cleanup()
