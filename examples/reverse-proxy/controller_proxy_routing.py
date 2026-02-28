"""
Controller with integrated reverse proxy — ZERO custom handler code.

The Controller just calls ``enable_proxy()`` and SubControllers configure
routes remotely via ``request_proxy_route()``.  The Controller handles
``_wire_proxy_route`` messages automatically as a built-in Wire protocol
feature.

Topology:

    Browser / curl
        |
        v
    Controller (WSS :8765 + HTTP proxy :8080)
        |            |            |
        v            v            v
    SubCtrl-A    SubCtrl-B    SubCtrl-C
    (API :3001)  (API :3002)  (Dashboard :3003)

When a SubController disconnects, its routes are automatically removed.

Usage:
    # Terminal 1 — start the controller
    python controller_proxy_routing.py

    # Terminal 2 — start a subcontroller (see subcontroller_announce.py)
    python subcontroller_announce.py

    # Terminal 3 — test
    curl http://localhost:8080/worker-a/api/health
"""

import asyncio

from wire import Controller, MessageType


async def main():
    ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="my-secret")
    await ctrl.start()

    # This is ALL you need on the Controller side.
    # SubControllers will configure routes remotely.
    await ctrl.enable_proxy(host="0.0.0.0", port=8080)

    # Optional: handle normal application messages
    @ctrl.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        print(f"JSON from {peer_fp[:16]}...: {data}")

    print("Controller running:")
    print("  WSS on wss://0.0.0.0:8765")
    print("  HTTP proxy on http://0.0.0.0:8080")
    print()
    print("SubControllers can configure proxy routes with one call:")
    print('  await sub.request_proxy_route("/my-api", my_fp, "http://host:port")')
    print()
    print("Routes are auto-removed when the SubController disconnects.")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await ctrl.stop()


if __name__ == "__main__":
    asyncio.run(main())
