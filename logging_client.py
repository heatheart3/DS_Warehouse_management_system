"""Logging service client for querying and analyzing log information."""

from __future__ import annotations

from typing import Optional

import grpc

import warehouse_pb2 as warehouse_pb2
import warehouse_pb2_grpc as warehouse_pb2_grpc


class LoggingClient:
    """Logging service client providing log query, statistics and management functionality."""
    
    def __init__(self, logger_endpoint: str):
        """Initialize logging client.
        
        Args:
            logger_endpoint: Logging service endpoint in format "host:port"
        """
        self.logger_endpoint = logger_endpoint
        self.channel = grpc.insecure_channel(logger_endpoint)
        self.stub = warehouse_pb2_grpc.LoggerServiceStub(self.channel)
    
    def close(self):
        """Close gRPC connection."""
        self.channel.close()
    
    def __enter__(self) -> "LoggingClient":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def query_logs(self, service_name: Optional[str] = None, operation: Optional[str] = None, limit: int = 10):
        """Query log records.
        
        Args:
            service_name: Service name filter (optional)
            operation: Operation type filter (optional)
            limit: Maximum number of records to return
            
        Returns:
            QueryLogsResponse: Query result
        """
        request = warehouse_pb2.QueryLogsRequest(
            service_name=service_name or "",
            operation=operation or "",
            limit=limit
        )
        return self.stub.QueryLogs(request)
    
    def get_stats(self):
        """Get statistics information.
        
        Returns:
            StatsResponse: Statistics information
        """
        request = warehouse_pb2.StatsRequest()
        return self.stub.GetStats(request)
    
    def clear_logs(self):
        """Clear log records.
        
        Returns:
            ClearLogsResponse: Clear operation result
        """
        request = warehouse_pb2.ClearLogsRequest()
        return self.stub.ClearLogs(request)
    
    def log_operation(self, service_name: str, operation: str, client_ip: str, 
                     success: bool, request_data: str = "", response_data: str = "", 
                     error_message: str = ""):
        """Manually log operation.
        
        Args:
            service_name: Service name
            operation: Operation type
            client_ip: Client IP address
            success: Whether operation was successful
            request_data: Request data (JSON string)
            response_data: Response data (JSON string)
            error_message: Error message (optional)
            
        Returns:
            LogResponse: Log operation response
        """
        request = warehouse_pb2.LogRequest(
            service_name=service_name,
            operation=operation,
            client_ip=client_ip,
            success=success,
            request_data=request_data,
            response_data=response_data,
            error_message=error_message
        )
        return self.stub.LogOperation(request)
    
    def print_recent_logs(self, limit: int = 10):
        """Print recent log records.
        
        Args:
            limit: Maximum number of records to display
        """
        try:
            logs_response = self.query_logs(limit=limit)
            print(f"📊 Found {logs_response.total_count} log records")
            
            if logs_response.logs:
                print("📋 Recent log records:")
                for i, log in enumerate(logs_response.logs[-limit:], 1):
                    status = "✅ Success" if log.success else "❌ Failed"
                    timestamp = log.timestamp.split('T')[1][:8] if 'T' in log.timestamp else log.timestamp
                    print(f"   {i}. [{timestamp}] {log.operation} - {status}")
                    if not log.success and log.error_message:
                        print(f"      Error: {log.error_message}")
            else:
                print("   📝 No log records found")
                
        except grpc.RpcError as e:
            print(f"⚠️ Unable to query logs: {e.details()}")
    
    def print_stats(self):
        """Print statistics information."""
        try:
            stats = self.get_stats()
            print(f"📈 Total operations: {stats.total_operations}")
            print(f"✅ Successful operations: {stats.successful_operations}")
            print(f"❌ Failed operations: {stats.failed_operations}")
            print(f"📊 Success rate: {stats.success_rate:.2f}%")
            
            if stats.service_stats:
                print("🏢 Statistics by service:")
                for service_stat in stats.service_stats:
                    print(f"   {service_stat.service_name}: {service_stat.total} operations, success rate {service_stat.success_rate:.2f}%")
            
            if stats.operation_stats:
                print("⚙️ Statistics by operation:")
                for op_stat in stats.operation_stats:
                    print(f"   {op_stat.operation}: {op_stat.total} operations, success rate {op_stat.success_rate:.2f}%")
                    
        except grpc.RpcError as e:
            print(f"⚠️ Unable to get statistics: {e.details()}")
    
