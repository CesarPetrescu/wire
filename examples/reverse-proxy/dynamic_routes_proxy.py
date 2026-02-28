"""
Dynamic route management — add and remove routes at runtime.

Demonstrates how to modify the proxy's route table while it is running.
This is useful when backends come and go (e.g. containers scaling up/down).

Usage:
    python dynamic_routes_proxy.py
"""

import asyncio

from wire import ReverseProxy


async def main():
    proxy = ReverseProxy(host="0.0.0.0", port=8080)

    # Start with a single backend
    proxy.add_route("/api", "http://localhost:3000")
    await proxy.start()
    print("Proxy started with /api -> http://localhost:3000")

    # Simulate a new service coming online after 5 seconds
    await asyncio.sleep(5)
    proxy.add_route("/metrics", "http://localhost:9090")
    print("Added /metrics -> http://localhost:9090")

    # Inspect current routes
    print("Current routes:", proxy.routes)

    # Simulate removing a route after another 10 seconds
    await asyncio.sleep(10)
    proxy.remove_route("/api")
    print("Removed /api route")
    print("Current routes:", proxy.routes)

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await proxy.stop()


if __name__ == "__main__":
    asyncio.run(main())
