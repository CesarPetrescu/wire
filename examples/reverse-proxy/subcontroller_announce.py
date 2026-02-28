"""
SubController that announces its HTTP service to the Controller's proxy.

This SubController runs a local HTTP API (simulated here) and tells the
Controller to register a proxy route for it.  Pair with
``controller_proxy_routing.py``.

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

    # Tell the Controller to route /worker-a/api to our local HTTP service
    await sub.send_json({
        "_announce_http": True,
        "path_prefix": "/worker-a/api",
        "upstream_url": "http://localhost:3001",  # our local HTTP service
    })

    @sub.on(MessageType.JSON)
    async def on_json(data):
        if data.get("proxy_registered"):
            print(
                f"Controller confirmed proxy route: "
                f"{data['path_prefix']}"
            )
        else:
            print(f"Received: {data}")

    print("SubController connected. HTTP service announced to Controller proxy.")
    print("Requests to http://controller:8080/worker-a/api/...")
    print("  will be forwarded to http://localhost:3001/...")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await sub.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
