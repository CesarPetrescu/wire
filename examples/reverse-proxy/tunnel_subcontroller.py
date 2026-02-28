"""
SubController that opens a proxy tunnel through the Wire mesh.

Calls ``open_proxy_tunnel()`` which:
  1. Starts a local HTTP listener on 0.0.0.0:9090
  2. Forwards all requests matching /api/* through the Wire mesh
     to the target peer (another SubController or the Controller)
  3. The target peer makes the actual HTTP call to the upstream URL
  4. The response flows back through Wire to the local HTTP client

Topology::

    curl http://localhost:9090/api/users
        |
        v
    THIS SubController (HTTP listener :9090)
        |  <-- Wire mesh (relay or direct) -->
        v
    Target peer (Controller or SubController)
        |
        v
    http://httpbin.org/anything/users  (the real backend)

Usage:
    # Terminal 1 — controller
    python tunnel_controller.py

    # Terminal 2 — this subcontroller
    python tunnel_subcontroller.py

    # Terminal 3 — test
    curl http://localhost:9090/api/anything
"""

import asyncio

from wire import SubController


async def main():
    sub = SubController(
        controller_url="wss://localhost:8765",
        preshared_secret="my-secret",
    )
    await sub.connect()

    # Open a tunnel: listen locally, forward through Wire to the Controller,
    # which makes the actual HTTP request to the upstream.
    tunnel = await sub.open_proxy_tunnel(
        listen_host="0.0.0.0",
        listen_port=9090,
        path_prefix="/api",
        target_fp=sub.controller_fingerprint,  # route to the Controller
        upstream_url="http://httpbin.org",      # the real backend
    )

    print("Proxy tunnel open!")
    print()
    print("  curl http://localhost:9090/api/get")
    print("    -> Wire -> Controller -> http://httpbin.org/get")
    print()
    print("  curl http://localhost:9090/api/post -d '{\"hello\":\"world\"}'")
    print("    -> Wire -> Controller -> http://httpbin.org/post")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await sub.close_proxy_tunnel(tunnel)
        await sub.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
