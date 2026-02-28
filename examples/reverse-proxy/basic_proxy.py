"""
Basic reverse proxy — single backend.

Exposes a backend service running on port 3000 through the proxy on port 8080.
All requests to http://localhost:8080/* are forwarded to http://localhost:3000/*.

Usage:
    python basic_proxy.py
"""

import asyncio

from wire import ReverseProxy


async def main():
    proxy = ReverseProxy(host="0.0.0.0", port=8080)

    # Forward everything to a single backend
    proxy.add_route("/", "http://localhost:3000")

    await proxy.start()
    print("Proxy running on http://0.0.0.0:8080 -> http://localhost:3000")

    try:
        await asyncio.Future()  # run forever
    except KeyboardInterrupt:
        pass
    finally:
        await proxy.stop()


if __name__ == "__main__":
    asyncio.run(main())
