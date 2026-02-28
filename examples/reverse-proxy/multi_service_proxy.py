"""
Multi-service reverse proxy — route by path prefix.

Expose multiple backend services through a single entry point.
Requests are dispatched based on the URL path prefix:

    /api/*        -> http://localhost:3000  (REST API)
    /auth/*       -> http://localhost:3001  (Auth service)
    /dashboard/*  -> http://localhost:8081  (Frontend dashboard)
    /*            -> http://localhost:8082  (Default / landing page)

Usage:
    python multi_service_proxy.py
"""

import asyncio

from wire import ReverseProxy


async def main():
    proxy = ReverseProxy(host="0.0.0.0", port=8080)

    # Most specific prefixes first (order doesn't matter — longest prefix wins)
    proxy.add_route("/api", "http://localhost:3000")
    proxy.add_route("/auth", "http://localhost:3001")
    proxy.add_route("/dashboard", "http://localhost:8081")

    # Catch-all for anything else
    proxy.add_route("/", "http://localhost:8082")

    await proxy.start()
    print("Multi-service proxy running on http://0.0.0.0:8080")
    print("  /api/*       -> http://localhost:3000")
    print("  /auth/*      -> http://localhost:3001")
    print("  /dashboard/* -> http://localhost:8081")
    print("  /*           -> http://localhost:8082")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await proxy.stop()


if __name__ == "__main__":
    asyncio.run(main())
