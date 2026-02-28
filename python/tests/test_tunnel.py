"""
Tests for the ProxyTunnel — HTTP tunnels through the Wire mesh.

Tests verify that open_proxy_tunnel() correctly:
  1. Starts a local HTTP listener on the calling node
  2. Forwards requests through Wire to the target peer
  3. The target peer makes the upstream HTTP call
  4. Responses flow back to the original HTTP client
"""

import asyncio
import json
import tempfile

import pytest
import pytest_asyncio
from aiohttp import ClientSession, web

from wire.controller import Controller
from wire.subcontroller import SubController
from wire.protocol import MessageType

# ── port allocation ──────────────────────────────────────────────────────────

PORT_BASE = 23000


def _next_port():
    _next_port.counter += 1
    return PORT_BASE + _next_port.counter


_next_port.counter = 0


# ── mock upstream HTTP server ─────────────────────────────────────────────────


def _make_upstream_app():
    """Tiny aiohttp app that echoes request info back as JSON."""
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


# ── Wire fixtures ─────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def wire_pair():
    """Controller + one SubController."""
    ws_port = _next_port()
    ctrl = Controller(
        host="127.0.0.1",
        port=ws_port,
        preshared_secret="tunnel-test",
        cert_dir=tempfile.mkdtemp(prefix="wire_ctrl_"),
    )
    sub = SubController(
        controller_url=f"wss://127.0.0.1:{ws_port}",
        preshared_secret="tunnel-test",
        cert_dir=tempfile.mkdtemp(prefix="wire_sub_"),
    )
    await ctrl.start()
    await sub.connect()
    await asyncio.sleep(0.1)
    yield ctrl, sub
    await sub.disconnect()
    await ctrl.stop()


@pytest_asyncio.fixture
async def wire_trio():
    """Controller + two SubControllers."""
    ws_port = _next_port()
    ctrl = Controller(
        host="127.0.0.1",
        port=ws_port,
        preshared_secret="tunnel-test",
        cert_dir=tempfile.mkdtemp(prefix="wire_ctrl_"),
    )
    sub_a = SubController(
        controller_url=f"wss://127.0.0.1:{ws_port}",
        preshared_secret="tunnel-test",
        cert_dir=tempfile.mkdtemp(prefix="wire_sub_a_"),
    )
    sub_b = SubController(
        controller_url=f"wss://127.0.0.1:{ws_port}",
        preshared_secret="tunnel-test",
        cert_dir=tempfile.mkdtemp(prefix="wire_sub_b_"),
    )
    await ctrl.start()
    await sub_a.connect()
    await sub_b.connect()
    await asyncio.sleep(0.2)
    yield ctrl, sub_a, sub_b
    await sub_a.disconnect()
    await sub_b.disconnect()
    await ctrl.stop()


# ── tests: SubController → Controller tunnel ──────────────────────────────────


@pytest.mark.asyncio
async def test_tunnel_sub_to_controller_get(wire_pair, upstream):
    """SubController opens tunnel → Controller → upstream. GET works."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{tunnel_port}/api/hello"
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["method"] == "GET"
                assert data["path"] == "/hello"
    finally:
        await sub.close_proxy_tunnel(tunnel)


@pytest.mark.asyncio
async def test_tunnel_sub_to_controller_post(wire_pair, upstream):
    """POST body is forwarded through the tunnel."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        body = json.dumps({"key": "value"})
        async with ClientSession() as client:
            async with client.post(
                f"http://127.0.0.1:{tunnel_port}/api/data",
                data=body,
                headers={"Content-Type": "application/json"},
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["method"] == "POST"
                assert data["body"] == body
    finally:
        await sub.close_proxy_tunnel(tunnel)


@pytest.mark.asyncio
async def test_tunnel_query_string(wire_pair, upstream):
    """Query parameters are preserved through the tunnel."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{tunnel_port}/api/search?q=hello&page=2"
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["query"] == "q=hello&page=2"
    finally:
        await sub.close_proxy_tunnel(tunnel)


@pytest.mark.asyncio
async def test_tunnel_put_delete(wire_pair, upstream):
    """PUT and DELETE methods work through the tunnel."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:
            async with client.put(
                f"http://127.0.0.1:{tunnel_port}/api/item/1",
                data=b"updated",
            ) as resp:
                data = await resp.json()
                assert data["method"] == "PUT"
                assert data["body"] == "updated"

            async with client.delete(
                f"http://127.0.0.1:{tunnel_port}/api/item/1"
            ) as resp:
                data = await resp.json()
                assert data["method"] == "DELETE"
    finally:
        await sub.close_proxy_tunnel(tunnel)


@pytest.mark.asyncio
async def test_tunnel_502_unreachable_upstream(wire_pair):
    """When the upstream is dead, the tunnel returns 502."""
    ctrl, sub = wire_pair

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/dead",
        target_fp=sub.controller_fingerprint,
        upstream_url="http://127.0.0.1:1",  # nothing listening
    )

    try:
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{tunnel_port}/dead/test"
            ) as resp:
                assert resp.status == 502
    finally:
        await sub.close_proxy_tunnel(tunnel)


@pytest.mark.asyncio
async def test_tunnel_404_no_matching_prefix(wire_pair, upstream):
    """Request to a path outside the tunnel prefix returns 404."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{tunnel_port}/other/path"
            ) as resp:
                assert resp.status == 404
    finally:
        await sub.close_proxy_tunnel(tunnel)


# ── tests: Controller → SubController tunnel ──────────────────────────────────


@pytest.mark.asyncio
async def test_tunnel_controller_to_sub(wire_pair, upstream):
    """Controller opens tunnel → SubController → upstream."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream
    sub_fp = sub.cert_bundle.fingerprint

    tunnel_port = _next_port()
    tunnel = await ctrl.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/svc",
        target_fp=sub_fp,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{tunnel_port}/svc/hello"
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["method"] == "GET"
                assert data["path"] == "/hello"
    finally:
        await ctrl.close_proxy_tunnel(tunnel)


# ── tests: SubController → SubController tunnel (via relay) ───────────────────


@pytest.mark.asyncio
async def test_tunnel_sub_to_sub_via_relay(wire_trio, upstream):
    """SubA opens tunnel → relay → SubB → upstream."""
    ctrl, sub_a, sub_b = wire_trio
    up_host, up_port = upstream
    sub_b_fp = sub_b.cert_bundle.fingerprint

    tunnel_port = _next_port()
    tunnel = await sub_a.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/peer",
        target_fp=sub_b_fp,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:
            async with client.get(
                f"http://127.0.0.1:{tunnel_port}/peer/relayed"
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["method"] == "GET"
                assert data["path"] == "/relayed"
    finally:
        await sub_a.close_proxy_tunnel(tunnel)


@pytest.mark.asyncio
async def test_tunnel_sub_to_sub_post_via_relay(wire_trio, upstream):
    """POST body survives the SubA → relay → SubB → upstream path."""
    ctrl, sub_a, sub_b = wire_trio
    up_host, up_port = upstream
    sub_b_fp = sub_b.cert_bundle.fingerprint

    tunnel_port = _next_port()
    tunnel = await sub_a.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/peer",
        target_fp=sub_b_fp,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        body = json.dumps({"relayed": True})
        async with ClientSession() as client:
            async with client.post(
                f"http://127.0.0.1:{tunnel_port}/peer/data",
                data=body,
                headers={"Content-Type": "application/json"},
            ) as resp:
                assert resp.status == 200
                data = await resp.json()
                assert data["method"] == "POST"
                assert data["body"] == body
    finally:
        await sub_a.close_proxy_tunnel(tunnel)


# ── tests: multiple concurrent requests ───────────────────────────────────────


@pytest.mark.asyncio
async def test_tunnel_concurrent_requests(wire_pair, upstream):
    """Multiple requests through the same tunnel work concurrently."""
    ctrl, sub = wire_pair
    up_host, up_port = upstream

    tunnel_port = _next_port()
    tunnel = await sub.open_proxy_tunnel(
        listen_host="127.0.0.1",
        listen_port=tunnel_port,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,
        upstream_url=f"http://{up_host}:{up_port}",
    )

    try:
        async with ClientSession() as client:

            async def make_request(i):
                async with client.get(
                    f"http://127.0.0.1:{tunnel_port}/api/item/{i}"
                ) as resp:
                    assert resp.status == 200
                    data = await resp.json()
                    assert data["path"] == f"/item/{i}"
                    return data

            results = await asyncio.gather(*[make_request(i) for i in range(5)])
            assert len(results) == 5
    finally:
        await sub.close_proxy_tunnel(tunnel)
