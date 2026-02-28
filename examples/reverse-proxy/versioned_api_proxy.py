"""
API versioning with reverse proxy — route /api/v1 and /api/v2 to different backends.

Longest-prefix matching ensures that /api/v2/* goes to the v2 backend
while /api/v1/* (or any other /api/* path) goes to the v1 backend.

Usage:
    python versioned_api_proxy.py
"""

import asyncio

from wire import ReverseProxy


async def main():
    proxy = ReverseProxy(host="0.0.0.0", port=8080)

    # v2 has its own backend; v1 is the default for /api
    proxy.add_route("/api/v2", "http://localhost:3002")
    proxy.add_route("/api", "http://localhost:3001")

    await proxy.start()
    print("Versioned API proxy running on http://0.0.0.0:8080")
    print("  /api/v2/* -> http://localhost:3002  (new backend)")
    print("  /api/*    -> http://localhost:3001  (legacy backend)")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await proxy.stop()


if __name__ == "__main__":
    asyncio.run(main())
