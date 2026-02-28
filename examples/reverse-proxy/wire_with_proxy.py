"""
Combined Wire Controller + Reverse Proxy.

Run a Wire WebSocket controller alongside an HTTP reverse proxy so that
a single server process manages both the real-time WebSocket channel and
HTTP traffic routing.

Usage:
    python wire_with_proxy.py
"""

import asyncio

from wire import Controller, MessageType, ReverseProxy


async def main():
    # --- Wire controller (WebSocket) ---
    ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="my-secret")

    @ctrl.on(MessageType.JSON)
    async def on_json(peer_fp, data):
        print(f"WS JSON from {peer_fp[:16]}: {data}")
        await ctrl.send_json(peer_fp, {"status": "ok"})

    # --- HTTP reverse proxy ---
    proxy = ReverseProxy(host="0.0.0.0", port=8080)
    proxy.add_route("/api", "http://localhost:3000")
    proxy.add_route("/dashboard", "http://localhost:8081")

    # Start both
    await ctrl.start()
    await proxy.start()
    print("Wire controller on wss://0.0.0.0:8765")
    print("HTTP proxy on http://0.0.0.0:8080")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await proxy.stop()
        await ctrl.stop()


if __name__ == "__main__":
    asyncio.run(main())
