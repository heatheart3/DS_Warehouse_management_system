"""gRPC server for the logistics inventory service."""

from __future__ import annotations

import argparse
import logging
import os
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

    def __init__(self, store: Optional[InventoryStore] = None, logger_endpoint: Optional[str] = None) -> None:
        self._store = store or InventoryStore()
        self._logger_endpoint = logger_endpoint

    def _get_client_ip(self, context) -> str:
        """Get client IP from gRPC context."""
        metadata = dict(context.invocation_metadata())
        return metadata.get('client-ip', 'unknown')

    def _log_operation(self, service_name: str, operation: str, client_ip: str, 
                      success: bool, request, response=None, error_message: str = ""):
        """Log operation to external logger service."""
        if not self._logger_endpoint:
            return
            
        import json
        request_data = json.dumps({
            "sku": getattr(request, 'sku', ''),
            "name": getattr(request, 'name', ''),
            "description": getattr(request, 'description', ''),
            "quantity": getattr(request, 'quantity', 0),
            "amount": getattr(request, 'amount', 0),
            "item": {
                "sku": getattr(request.item, 'sku', '') if hasattr(request, 'item') else '',
                "name": getattr(request.item, 'name', '') if hasattr(request, 'item') else '',
                "description": getattr(request.item, 'description', '') if hasattr(request, 'item') else '',
                "quantity": getattr(request.item, 'quantity', 0) if hasattr(request, 'item') else 0,
            } if hasattr(request, 'item') else {}
        }, default=str)
        
        response_data = ""
        if response:
            response_data = json.dumps({
                "item": {
                    "sku": getattr(response.item, 'sku', '') if hasattr(response, 'item') else '',
                    "name": getattr(response.item, 'name', '') if hasattr(response, 'item') else '',
                    "description": getattr(response.item, 'description', '') if hasattr(response, 'item') else '',
                    "quantity": getattr(response.item, 'quantity', 0) if hasattr(response, 'item') else 0,
                } if hasattr(response, 'item') else {}
            }, default=str)
        
        log_request = warehouse_pb2.LogRequest(
            service_name=service_name,
            operation=operation,
            client_ip=client_ip,
            success=success,
            request_data=request_data,
            response_data=response_data,
            error_message=error_message
        )
        
        try:
            channel = grpc.insecure_channel(self._logger_endpoint)
            stub = warehouse_pb2_grpc.LoggerServiceStub(channel)
            stub.LogOperation(log_request)
            channel.close()
        except Exception as e:
            logging.error(f"Failed to send log to external service: {e}")

    def AddItem(self, request, context):  # pylint: disable=invalid-name
        """Add item to inventory."""
        client_ip = self._get_client_ip(context)
        success = False
        error_message = ""
        
        try:
            item = self._store.add_item(request.item)
            success = True
            response = warehouse_pb2.AddItemResponse(item=item)
            self._log_operation("InventoryService", "AddItem", client_ip, success, request, response)
            return response
        except ValueError as exc:  # empty SKU or invalid quantity
            error_message = str(exc)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemAlreadyExistsError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(exc))
        finally:
            if not success:
                self._log_operation("InventoryService", "AddItem", client_ip, success, request, None, error_message)

    def UpdateItem(self, request, context):  # pylint: disable=invalid-name
        """Update item information."""
        client_ip = self._get_client_ip(context)
        success = False
        error_message = ""
        
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
            success = True
            response = warehouse_pb2.UpdateItemResponse(item=item)
            self._log_operation("InventoryService", "UpdateItem", client_ip, success, request, response)
            return response
        except ValueError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemNotFoundError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        finally:
            if not success:
                self._log_operation("InventoryService", "UpdateItem", client_ip, success, request, None, error_message)

    def TakeItem(self, request, context):  # pylint: disable=invalid-name
        """Take item from inventory."""
        client_ip = self._get_client_ip(context)
        success = False
        error_message = ""
        
        try:
            item = self._store.take_item(request.sku, request.amount)
            success = True
            response = warehouse_pb2.TakeItemResponse(item=item)
            self._log_operation("InventoryService", "TakeItem", client_ip, success, request, response)
            return response
        except ValueError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemNotFoundError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        except InsufficientQuantityError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(exc))
        finally:
            if not success:
                self._log_operation("InventoryService", "TakeItem", client_ip, success, request, None, error_message)

    def QueryItem(self, request, context):  # pylint: disable=invalid-name
        """Query item information."""
        client_ip = self._get_client_ip(context)
        success = False
        error_message = ""
        
        try:
            item = self._store.query_item(request.sku)
            success = True
            response = warehouse_pb2.QueryItemResponse(item=item)
            self._log_operation("InventoryService", "QueryItem", client_ip, success, request, response)
            return response
        except ValueError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except ItemNotFoundError as exc:
            error_message = str(exc)
            context.abort(grpc.StatusCode.NOT_FOUND, str(exc))
        finally:
            if not success:
                self._log_operation("InventoryService", "QueryItem", client_ip, success, request, None, error_message)


def create_server_at_endpoint(
    endpoint: str,
    store: Optional[InventoryStore] = None,
    max_workers: int = 16,
    logger_endpoint: Optional[str] = None,
):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    
    # Create inventory service with external logging
    service = InventoryService(store=store, logger_endpoint=logger_endpoint)
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


def create_server(host: str, port: int, store: Optional[InventoryStore] = None, max_workers: int = 16, logger_endpoint: Optional[str] = None):
    endpoint = f"{host}:{port}"
    return create_server_at_endpoint(endpoint, store=store, max_workers=max_workers, logger_endpoint=logger_endpoint)


def main() -> None:
    parser = argparse.ArgumentParser(description="InventoryService gRPC server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=50051, help="Bind port")
    parser.add_argument(
        "--max-workers", type=int, default=16, help="Thread pool worker count"
    )
    parser.add_argument(
        "--logger-endpoint", help="External logger service endpoint (optional)"
    )
    args = parser.parse_args()

    # Check for logger endpoint from environment variable
    logger_endpoint = args.logger_endpoint or os.environ.get("LOGGER_ENDPOINT")

    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    server = create_server(args.host, args.port, max_workers=args.max_workers, logger_endpoint=logger_endpoint)
    server.start()
    endpoint = getattr(server, "bound_endpoint", f"{args.host}:{args.port}")
    logging.info("InventoryService listening on %s", endpoint)
    
    if logger_endpoint:
        logging.info("Using external logger service at %s", logger_endpoint)
    else:
        logging.info("No logger service configured")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info("Shutting down server...")
        server.stop(grace=None)


if __name__ == "__main__":
    main()
