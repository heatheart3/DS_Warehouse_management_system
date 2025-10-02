"""Example client that exercises the distributed inventory cluster."""

from __future__ import annotations

import json
import os
import sys
from typing import List

import grpc

from distributed_inventory import DistributedInventoryClient


def _load_endpoints() -> List[str]:
    raw = os.environ.get("WAREHOUSE_ENDPOINTS")
    if not raw:
        raise SystemExit("WAREHOUSE_ENDPOINTS environment variable is required")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Invalid WAREHOUSE_ENDPOINTS JSON: {exc}") from exc
    if not isinstance(data, list) or not all(isinstance(item, str) for item in data):
        raise SystemExit("WAREHOUSE_ENDPOINTS must be a JSON list of endpoint strings")
    return data


def main() -> None:
    endpoints = _load_endpoints()
    print(f"Connecting to warehouse nodes: {endpoints}")

    try:
        with DistributedInventoryClient(endpoints) as client:
            sku = "DEMO-001"
            print(f"Adding SKU {sku}")
            client.add_item(sku, name="Demo Item", description="Seed item", quantity=500)

            item = client.query_item(sku)
            print(f"Queried item: quantity={item.quantity}")

            print("Updating quantity to 480")
            client.update_item(sku, quantity=480)
            item = client.query_item(sku)
            print(f"After update: quantity={item.quantity}")

            print("Taking 130 units")
            client.take_item(sku, amount=130)
            item = client.query_item(sku)
            print(f"After take: quantity={item.quantity}")

    except grpc.RpcError as exc:  # pragma: no cover - runtime logging
        print(f"gRPC error from cluster: {exc.code()} {exc.details()}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
