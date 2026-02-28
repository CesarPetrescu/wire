"""
Controller that serves as a tunnel target.

SubControllers will open proxy tunnels pointing at this Controller (or
through it to other SubControllers).  The Controller just starts normally
— tunnel requests are handled automatically by the built-in protocol.

Usage:
    # Terminal 1 — start the controller
    python tunnel_controller.py

    # Terminal 2 — start a subcontroller that opens a tunnel
    python tunnel_subcontroller.py

    # Terminal 3 — test
    curl http://localhost:9090/api/anything
"""

import asyncio

from wire import Controller


async def main():
    ctrl = Controller(host="0.0.0.0", port=8765, preshared_secret="my-secret")
    await ctrl.start()

    print("Controller running on wss://0.0.0.0:8765")
    print()
    print("SubControllers can open proxy tunnels through me.")
    print("Tunnel requests (_wire_tunnel_req) are handled automatically.")

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        pass
    finally:
        await ctrl.stop()


if __name__ == "__main__":
    asyncio.run(main())
