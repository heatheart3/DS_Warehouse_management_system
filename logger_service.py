"""日志服务实现，用于记录和查询系统操作日志。"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

import warehouse_pb2 as warehouse_pb2
import warehouse_pb2_grpc as warehouse_pb2_grpc


class LoggerService(warehouse_pb2_grpc.LoggerServiceServicer):
    """日志服务实现，提供日志记录、查询和统计功能。"""

    def __init__(self) -> None:
        """初始化日志服务。"""
        self._logs: List[warehouse_pb2.LogEntry] = []
        self._logger = logging.getLogger(__name__)

    def LogOperation(self, request: warehouse_pb2.LogRequest, context) -> warehouse_pb2.LogResponse:
        """
        记录操作日志。
        
        Args:
            request: 日志记录请求
            context: gRPC 上下文
            
        Returns:
            LogResponse: 日志记录响应
        """
        try:
            # 创建日志条目
            log_entry = warehouse_pb2.LogEntry(
                timestamp=datetime.now().isoformat(),
                service_name=request.service_name,
                operation=request.operation,
                client_ip=request.client_ip,
                success=request.success,
                request_data=request.request_data,
                response_data=request.response_data,
                error_message=request.error_message
            )
            
            # 添加到日志列表
            self._logs.append(log_entry)
            
            # 记录到系统日志
            self._logger.info(
                f"Operation logged: {request.service_name}.{request.operation} "
                f"from {request.client_ip} - {'SUCCESS' if request.success else 'FAILED'}"
            )
            
            return warehouse_pb2.LogResponse(
                success=True,
                message="Operation logged successfully"
            )
            
        except Exception as e:
            self._logger.error(f"Failed to log operation: {e}")
            return warehouse_pb2.LogResponse(
                success=False,
                message=f"Failed to log operation: {str(e)}"
            )

    def QueryLogs(self, request: warehouse_pb2.QueryLogsRequest, context) -> warehouse_pb2.QueryLogsResponse:
        """
        查询日志记录。
        
        Args:
            request: 查询日志请求
            context: gRPC 上下文
            
        Returns:
            QueryLogsResponse: 查询日志响应
        """
        try:
            # 过滤日志
            filtered_logs = []
            for log in self._logs:
                # 按服务名称过滤
                if request.service_name and log.service_name != request.service_name:
                    continue
                # 按操作类型过滤
                if request.operation and log.operation != request.operation:
                    continue
                filtered_logs.append(log)
            
            # 应用数量限制
            if request.limit > 0:
                filtered_logs = filtered_logs[-request.limit:]  # 获取最新的记录
            
            return warehouse_pb2.QueryLogsResponse(
                logs=filtered_logs,
                total_count=len(filtered_logs)
            )
            
        except Exception as e:
            self._logger.error(f"Failed to query logs: {e}")
            return warehouse_pb2.QueryLogsResponse(
                logs=[],
                total_count=0
            )

    def GetStats(self, request: warehouse_pb2.StatsRequest, context) -> warehouse_pb2.StatsResponse:
        """
        获取统计信息。
        
        Args:
            request: 统计信息请求
            context: gRPC 上下文
            
        Returns:
            StatsResponse: 统计信息响应
        """
        try:
            total_operations = len(self._logs)
            successful_operations = sum(1 for log in self._logs if log.success)
            failed_operations = total_operations - successful_operations
            success_rate = (successful_operations / total_operations * 100) if total_operations > 0 else 0.0
            
            # 按服务统计
            service_stats = self._calculate_service_stats()
            
            # 按操作统计
            operation_stats = self._calculate_operation_stats()
            
            return warehouse_pb2.StatsResponse(
                total_operations=total_operations,
                successful_operations=successful_operations,
                failed_operations=failed_operations,
                success_rate=success_rate,
                service_stats=service_stats,
                operation_stats=operation_stats
            )
            
        except Exception as e:
            self._logger.error(f"Failed to get stats: {e}")
            return warehouse_pb2.StatsResponse(
                total_operations=0,
                successful_operations=0,
                failed_operations=0,
                success_rate=0.0,
                service_stats=[],
                operation_stats=[]
            )

    def ClearLogs(self, request: warehouse_pb2.ClearLogsRequest, context) -> warehouse_pb2.ClearLogsResponse:
        """
        清空日志记录。
        
        Args:
            request: 清空日志请求
            context: gRPC 上下文
            
        Returns:
            ClearLogsResponse: 清空日志响应
        """
        try:
            cleared_count = len(self._logs)
            self._logs.clear()
            
            self._logger.info(f"Cleared {cleared_count} log entries")
            
            return warehouse_pb2.ClearLogsResponse(
                success=True,
                message=f"Cleared {cleared_count} log entries",
                cleared_count=cleared_count
            )
            
        except Exception as e:
            self._logger.error(f"Failed to clear logs: {e}")
            return warehouse_pb2.ClearLogsResponse(
                success=False,
                message=f"Failed to clear logs: {str(e)}",
                cleared_count=0
            )

    def _calculate_service_stats(self) -> List[warehouse_pb2.ServiceStats]:
        """计算服务统计信息。"""
        service_counts: Dict[str, Dict[str, int]] = {}
        
        for log in self._logs:
            service_name = log.service_name
            if service_name not in service_counts:
                service_counts[service_name] = {"total": 0, "success": 0, "failed": 0}
            
            service_counts[service_name]["total"] += 1
            if log.success:
                service_counts[service_name]["success"] += 1
            else:
                service_counts[service_name]["failed"] += 1
        
        service_stats = []
        for service_name, counts in service_counts.items():
            success_rate = (counts["success"] / counts["total"] * 100) if counts["total"] > 0 else 0.0
            service_stats.append(warehouse_pb2.ServiceStats(
                service_name=service_name,
                total=counts["total"],
                success=counts["success"],
                failed=counts["failed"],
                success_rate=success_rate
            ))
        
        return service_stats

    def _calculate_operation_stats(self) -> List[warehouse_pb2.OperationStats]:
        """计算操作统计信息。"""
        operation_counts: Dict[str, Dict[str, int]] = {}
        
        for log in self._logs:
            operation = log.operation
            if operation not in operation_counts:
                operation_counts[operation] = {"total": 0, "success": 0, "failed": 0}
            
            operation_counts[operation]["total"] += 1
            if log.success:
                operation_counts[operation]["success"] += 1
            else:
                operation_counts[operation]["failed"] += 1
        
        operation_stats = []
        for operation, counts in operation_counts.items():
            success_rate = (counts["success"] / counts["total"] * 100) if counts["total"] > 0 else 0.0
            operation_stats.append(warehouse_pb2.OperationStats(
                operation=operation,
                total=counts["total"],
                success=counts["success"],
                failed=counts["failed"],
                success_rate=success_rate
            ))
        
        return operation_stats


def run_logger_service(port=50060):
    """Run LoggerService standalone server."""
    import grpc
    from concurrent import futures
    import time
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    warehouse_pb2_grpc.add_LoggerServiceServicer_to_server(LoggerService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f"📊 LoggerService started on port {port}")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Stopping LoggerService...")
        server.stop(0)


if __name__ == "__main__":
    run_logger_service()
