import asyncio
import logging
import argparse
from mamamia.server.registry import LogRegistry
from mamamia.server.tcp import TcpFrontend


async def main():
    parser = argparse.ArgumentParser(description="Mamamia Message Delivery Server")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=9000, help="Port to bind to")
    parser.add_argument(
        "--reaper-interval",
        type=float,
        default=30.0,
        help="Lease reaper interval in seconds",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level")

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    registry = LogRegistry()
    registry.start_reaper(interval=args.reaper_interval)

    server = TcpFrontend(registry, host=args.host, port=args.port)

    print(f"Starting Mamamia Server on {args.host}:{args.port}...")
    try:
        await server.start()
    except asyncio.CancelledError:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
