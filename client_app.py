"""Example client that exercises the distributed inventory cluster with logging functionality."""

from __future__ import annotations

import json
import os
import sys
import time
from typing import List, Optional

import grpc

from distributed_inventory import DistributedInventoryClient
from logging_client import LoggingClient


def _load_endpoints() -> List[str]:
    """Load inventory service endpoints configuration."""
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

def _load_logger_endpoint() -> str:
    """Load logger service endpoint configuration."""
    return os.environ.get("LOGGER_ENDPOINT", "localhost:50052")

def main() -> None:
    """Main function that executes inventory operations and tests logging functionality."""
    endpoints = _load_endpoints()
    logger_endpoint = _load_logger_endpoint()
    
    print("🚀 DS_Warehouse_management_system Client Starting")
    print("=" * 60)
    print(f"📦 Inventory Service Endpoints: {endpoints}")
    print(f"📊 Logger Service Endpoint: {logger_endpoint}")
    print("=" * 60)

    try:
        with DistributedInventoryClient(endpoints) as inventory_client:
            with LoggingClient(logger_endpoint) as logging_client:
                # 1. Execute inventory operations
                print("\n1️⃣ Executing inventory operations...")
                sku = "DEMO-001"
                print(f"   📦 Adding item: {sku}")
                inventory_client.add_item(sku, name="Demo Item", description="Seed item", quantity=500)

                item = inventory_client.query_item(sku)
                print(f"   🔍 Query item: quantity={item.quantity}")

                print("   ✏️ Updating quantity to 480")
                inventory_client.update_item(sku, quantity=480)
                item = inventory_client.query_item(sku)
                print(f"   📊 After update: quantity={item.quantity}")

                print("   📤 Taking 130 units")
                inventory_client.take_item(sku, amount=130)
                item = inventory_client.query_item(sku)
                print(f"   📊 After take: quantity={item.quantity}")
                
                # 2. Test error scenarios
                print("\n2️⃣ Testing error scenarios...")
                try:
                    inventory_client.take_item(sku, amount=500)  # Exceed inventory
                except grpc.RpcError as e:
                    print(f"   ❌ Expected error: {e.details()}")
                
                # 3. Query logs
                print("\n4️⃣ Querying operation logs...")
                logging_client.print_recent_logs(limit=10)
                
                # 4. Get statistics
                print("\n5️⃣ Getting statistics...")
                logging_client.print_stats()
                
                # 5. Test log filtering
                print("\n6️⃣ Testing log filtering...")
                try:
                    # Filter by operation type
                    add_logs = logging_client.query_logs(operation="AddItem", limit=5)
                    print(f"   🔍 AddItem operations: {add_logs.total_count} records")
                    
                    # Filter by service name
                    inventory_logs = logging_client.query_logs(service_name="InventoryService", limit=5)
                    print(f"   🔍 InventoryService logs: {inventory_logs.total_count} records")
                    
                    # Filter by specific operations
                    query_logs = logging_client.query_logs(operation="QueryItem", limit=3)
                    print(f"   🔍 QueryItem operations: {query_logs.total_count} records")
                    
                    update_logs = logging_client.query_logs(operation="UpdateItem", limit=3)
                    print(f"   🔍 UpdateItem operations: {update_logs.total_count} records")
                    
                    take_logs = logging_client.query_logs(operation="TakeItem", limit=3)
                    print(f"   🔍 TakeItem operations: {take_logs.total_count} records")
                    
                except grpc.RpcError as e:
                    print(f"   ⚠️ Unable to filter logs: {e.details()}")
                
                # 6. Test log management operations
                print("\n7️⃣ Testing log management operations...")
                try:
                    # Test clearing logs
                    print("   🗑️ Testing clear logs functionality...")
                    clear_response = logging_client.clear_logs()
                    if clear_response.success:
                        print(f"   ✅ Successfully cleared {clear_response.cleared_count} log entries")
                    else:
                        print(f"   ❌ Failed to clear logs: {clear_response.message}")
                    
                    # Get stats after clearing
                    print("   📊 Statistics after clearing logs:")
                    logging_client.print_stats()
                    
                except grpc.RpcError as e:
                    print(f"   ⚠️ Unable to test log management: {e.details()}")
                
                # 7. Test direct log operation
                print("\n8️⃣ Testing direct log operation...")
                try:
                    # Log a test operation directly
                    test_response = logging_client.log_operation(
                        service_name="TestService",
                        operation="TestOperation", 
                        client_ip="127.0.0.1",
                        success=True,
                        request_data='{"test": "direct_log_test"}',
                        response_data='{"result": "success"}',
                        error_message=""
                    )
                    if test_response.success:
                        print("   ✅ Direct log operation successful")
                    else:
                        print(f"   ❌ Direct log operation failed: {test_response.message}")
                    
                    # Verify the logged operation
                    test_logs = logging_client.query_logs(service_name="TestService", limit=1)
                    print(f"   🔍 TestService logs: {test_logs.total_count} records")
                    
                except grpc.RpcError as e:
                    print(f"   ⚠️ Unable to test direct log operation: {e.details()}")
                
                # 8. Test error logging
                print("\n9️⃣ Testing error logging...")
                try:
                    # Log an error operation
                    error_response = logging_client.log_operation(
                        service_name="ErrorTestService",
                        operation="ErrorOperation",
                        client_ip="127.0.0.1", 
                        success=False,
                        request_data='{"test": "error_test"}',
                        response_data='{}',
                        error_message="Test error message for logging"
                    )
                    if error_response.success:
                        print("   ✅ Error log operation successful")
                    else:
                        print(f"   ❌ Error log operation failed: {error_response.message}")
                    
                    # Check final statistics
                    print("   📊 Final statistics:")
                    logging_client.print_stats()
                    
                except grpc.RpcError as e:
                    print(f"   ⚠️ Unable to test error logging: {e.details()}")
                
                print("\n" + "=" * 60)
                print("🎉 Client testing completed!")
                print("💡 Note: All inventory operations have been logged to the logging service")
                print("=" * 60)

    except grpc.RpcError as exc:  # pragma: no cover - runtime logging
        print(f"❌ gRPC Error: {exc.code()} {exc.details()}", file=sys.stderr)
        sys.exit(1)
    except Exception as exc:
        print(f"❌ Unexpected error: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
