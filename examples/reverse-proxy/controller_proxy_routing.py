"""
Controller with integrated reverse proxy — route HTTP traffic to SubController services.

This example shows the main use case: a Controller runs a reverse proxy that
routes HTTP requests to HTTP services running on (or alongside) its connected
SubControllers.

Topology:

    Browser / curl
        |
        v
    Controller (WSS :8765 + HTTP proxy :8080)
        |            |            |
        v            v            v
    SubCtrl-A    SubCtrl-B    SubCtrl-C
    (API :3001)  (API :3002)  (Dashboard :3003)

When a SubController connects, we register a proxy route.
When it disconnects, the route is automatically removed.

Usage:
    python controller_proxy_routing.py
"""

import asyncio
import json

from wire import Controller, MessageType


async def main():
    ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="my-secret")

    # Enable the reverse proxy on port 8080
    await ctrl.start()
    await ctrl.enable_proxy(host="0.0.0.0", port=8080)

    # Track which peer offers which service
    # In real usage, SubControllers would announce their HTTP endpoint
    # via a JSON message after connecting.

    @ctrl.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        # SubControllers announce their HTTP service with a special message
        if data.get("_announce_http"):
            path_prefix = data["path_prefix"]   # e.g. "/worker-a/api"
            upstream_url = data["upstream_url"]  # e.g. "http://10.0.0.5:3001"

            ctrl.add_proxy_route_for_peer(path_prefix, peer_fp, upstream_url)
            print(
                f"Registered proxy: {path_prefix} -> {upstream_url} "
                f"(peer {peer_fp[:16]}...)"
            )

            # Confirm to the SubController
            await ctrl.send_json(peer_fp, {
                "proxy_registered": True,
                "path_prefix": path_prefix,
            })
            return

        # Normal application messages
        print(f"JSON from {peer_fp[:16]}: {data}")

    print("Controller running:")
    print("  WSS on wss://0.0.0.0:8765")
    print("  HTTP proxy on http://0.0.0.0:8080")
    print()
    print("SubControllers can announce their HTTP services by sending:")
    print('  {"_announce_http": true, "path_prefix": "/my-api", '
          '"upstream_url": "http://host:port"}')
    print()
    print("The proxy will then forward:")
    print("  http://controller:8080/my-api/... -> http://host:port/...")
    print()
    print("When a SubController disconnects, its routes are auto-removed.")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await ctrl.stop()


if __name__ == "__main__":
    asyncio.run(main())
