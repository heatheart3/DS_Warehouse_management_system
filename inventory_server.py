"""gRPC server for the logistics inventory service."""

from __future__ import annotations

import argparse
import logging
from concurrent import futures
from typing import Optional

import grpc

import warehouse_pb2 as warehouse_pb2
import warehouse_pb2_grpc as warehouse_pb2_grpc
from inventory_store import (
    InsufficientQuantityError,
    InventoryStore,
    ItemAlreadyExistsError,
    ItemNotFoundError,
)


class InventoryService(warehouse_pb2_grpc.InventoryServiceServicer):
    """Implementation of the gRPC service backed by an InventoryStore."""

    def __init__(self, store: Optional[InventoryStore] = None) -> None:
        self._store = store or InventoryStore()

    def AddItem(self, request, context):  # pylint: disable=invalid-name
        try:
            item = self._store.add_item(request.item)
            return warehouse_pb2.AddItemResponse(item=item)
        except ValueError as exc:  # empty SKU or invalid quantity
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemAlreadyExistsError as exc:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))

    def UpdateItem(self, request, context):  # pylint: disable=invalid-name
        try:
            metadata = {key.lower(): value for key, value in context.invocation_metadata()}
            force_quantity = metadata.get("update-quantity", "").lower() in {"1", "true", "yes"}

            name = request.name if request.name else None
            description = request.description if request.description else None
            if force_quantity or request.quantity != 0:
                quantity = request.quantity
            else:
                quantity = None

            item = self._store.update_item(
                request.sku, name=name, description=description, quantity=quantity
            )
            return warehouse_pb2.UpdateItemResponse(item=item)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemNotFoundError as exc:
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))

    def TakeItem(self, request, context):  # pylint: disable=invalid-name
        try:
            item = self._store.take_item(request.sku, request.amount)
            return warehouse_pb2.TakeItemResponse(item=item)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemNotFoundError as exc:
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        except InsufficientQuantityError as exc:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))

    def QueryItem(self, request, context):  # pylint: disable=invalid-name
        try:
            item = self._store.query_item(request.sku)
            return warehouse_pb2.QueryItemResponse(item=item)
        except ValueError as exc:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemNotFoundError as exc:
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))


def create_server_at_endpoint(
    endpoint: str,
    store: Optional[InventoryStore] = None,
    max_workers: int = 16,
):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    service = InventoryService(store=store)
    warehouse_pb2_grpc.add_InventoryServiceServicer_to_server(service, server)
    bound = server.add_insecure_port(endpoint)
    if not bound:
        raise RuntimeError(f"Failed to bind InventoryService to {endpoint}")

    host, sep, _ = endpoint.rpartition(":")
    if endpoint.startswith("unix:") or not sep:
        actual_endpoint = endpoint
    else:
        actual_endpoint = f"{host}:{bound}"

    setattr(server, "bound_port", bound)
    setattr(server, "bound_endpoint", actual_endpoint)
    return server


def create_server(host: str, port: int, store: Optional[InventoryStore] = None, max_workers: int = 16):
    endpoint = f"{host}:{port}"
    return create_server_at_endpoint(endpoint, store=store, max_workers=max_workers)


def main() -> None:
    parser = argparse.ArgumentParser(description="InventoryService gRPC server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=50051, help="Bind port")
    parser.add_argument(
        "--max-workers", type=int, default=16, help="Thread pool worker count"
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    server = create_server(args.host, args.port, max_workers=args.max_workers)
    server.start()
    endpoint = getattr(server, "bound_endpoint", f"{args.host}:{args.port}")
    logging.info("InventoryService listening on %s", endpoint)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info("Shutting down server...")
        server.stop(grace=None)


if __name__ == "__main__":
    main()
