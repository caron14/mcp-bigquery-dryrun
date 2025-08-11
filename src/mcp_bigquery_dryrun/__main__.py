"""Main entry point for the MCP BigQuery Dry-Run server."""

import asyncio
import sys
from .server import main as server_main


def main():
    """Console script entry point."""
    try:
        asyncio.run(server_main())
    except KeyboardInterrupt:
        print("\nServer stopped by user", file=sys.stderr)
        sys.exit(0)
    except Exception as e:
        print(f"Server error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()