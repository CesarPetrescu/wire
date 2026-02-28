"""
SubController that registers a proxy route on the Controller — one call.

After connecting, calls ``request_proxy_route()`` which tells the Controller:
  "Route /worker-a/api on your proxy to http://localhost:3001, and tie that
   route to MY fingerprint (auto-remove when I disconnect)."

The Controller needs NO custom code — it handles the request automatically.

Usage:
    python subcontroller_announce.py
"""

import asyncio

from wire import MessageType, SubController


async def main():
    sub = SubController(
        controller_url="wss://localhost:8765",
        preshared_secret="my-secret",
    )
    await sub.connect()

    # One call configures everything on the Controller's proxy.
    # - path_prefix:  what URL prefix on the controller to listen on
    # - peer_fp:      whose lifecycle the route is tied to (ourselves)
    # - upstream_url: where the traffic actually goes
    await sub.request_proxy_route(
        "/worker-a/api",                    # path on controller's proxy
        sub.cert_bundle.fingerprint,        # bind to our own lifecycle
        "http://localhost:3001",            # our local HTTP service
    )

    @sub.on(MessageType.JSON)
    async def on_json(data):
        # The Controller sends back a confirmation
        result = data.get("_wire_proxy_route_result")
        if result:
            if result["ok"]:
                print(f"Route registered: {result['path_prefix']} -> {result['upstream_url']}")
            else:
                print(f"Route failed: {result['error']}")
        else:
            print(f"Received: {data}")

    print("SubController connected.")
    print("Requests to http://controller:8080/worker-a/api/...")
    print("  will be forwarded to http://localhost:3001/...")
    print()
    print("Disconnect (Ctrl+C) and the route is auto-removed.")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await sub.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
