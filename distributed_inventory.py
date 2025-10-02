"""Client helper that distributes inventory operations across gRPC nodes."""

from __future__ import annotations

import hashlib
from typing import Callable, List, Optional, Sequence, Tuple

import grpc

import warehouse_pb2 as warehouse_pb2
import warehouse_pb2_grpc as warehouse_pb2_grpc

Metadata = Optional[Sequence[Tuple[str, str]]]
StubWithCloser = Tuple[warehouse_pb2_grpc.InventoryServiceStub, Optional[Callable[[], None]]]


class DistributedInventoryClient:
    """Routes requests to InventoryService nodes using consistent hashing."""

    def __init__(
        self,
        endpoints: Sequence[str],
        timeout: float = 5.0,
        stub_factory: Optional[Callable[[str], StubWithCloser]] = None,
    ) -> None:
        if not endpoints:
            raise ValueError("At least one endpoint is required")
        self._timeout = timeout
        self._stubs: List[warehouse_pb2_grpc.InventoryServiceStub] = []
        self._endpoints: List[str] = []
        self._closers: List[Callable[[], None]] = []

        for endpoint in endpoints:
            if stub_factory:
                stub, closer = stub_factory(endpoint)
            else:
                channel = grpc.insecure_channel(endpoint)
                stub = warehouse_pb2_grpc.InventoryServiceStub(channel)
                closer = channel.close
            self._stubs.append(stub)
            self._endpoints.append(endpoint)
            self._closers.append(closer or (lambda: None))

    def close(self) -> None:
        for closer in self._closers:
            closer()
        self._closers.clear()

    def __enter__(self) -> "DistributedInventoryClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def _select_index(self, sku: str) -> int:
        digest = hashlib.sha256(sku.encode("utf-8")).digest()
        value = int.from_bytes(digest[:8], byteorder="big")
        return value % len(self._stubs)

    def endpoint_for_sku(self, sku: str) -> str:
        return self._endpoints[self._select_index(sku)]

    def _stub_for(self, sku: str) -> warehouse_pb2_grpc.InventoryServiceStub:
        return self._stubs[self._select_index(sku)]

    @staticmethod
    def _extend_metadata(metadata: Metadata, extra: Sequence[Tuple[str, str]]) -> Optional[List[Tuple[str, str]]]:
        if metadata:
            combined = list(metadata) + list(extra)
        else:
            combined = list(extra)
        return combined if combined else None

    def add_item(
        self,
        sku: str,
        name: str,
        description: str,
        quantity: int,
        metadata: Metadata = None,
    ) -> warehouse_pb2.Item:
        item = warehouse_pb2.Item(
            sku=sku,
            name=name,
            description=description,
            quantity=quantity,
        )
        stub = self._stub_for(sku)
        response = stub.AddItem(
            warehouse_pb2.AddItemRequest(item=item),
            timeout=self._timeout,
            metadata=metadata,
        )
        return response.item

    def update_item(
        self,
        sku: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        quantity: Optional[int] = None,
        metadata: Metadata = None,
    ) -> warehouse_pb2.Item:
        current = self.query_item(sku, metadata=metadata)
        name = current.name if name is None else name
        description = current.description if description is None else description
        quantity = current.quantity if quantity is None else quantity

        stub = self._stub_for(sku)
        effective_metadata = self._extend_metadata(metadata, [("update-quantity", "true")])
        request = warehouse_pb2.UpdateItemRequest(
            sku=sku,
            name=name,
            description=description,
            quantity=quantity,
        )
        response = stub.UpdateItem(
            request,
            timeout=self._timeout,
            metadata=effective_metadata,
        )
        return response.item

    def take_item(
        self,
        sku: str,
        amount: int,
        metadata: Metadata = None,
    ) -> warehouse_pb2.Item:
        stub = self._stub_for(sku)
        request = warehouse_pb2.TakeItemRequest(sku=sku, amount=amount)
        response = stub.TakeItem(
            request,
            timeout=self._timeout,
            metadata=metadata,
        )
        return response.item

    def query_item(
        self,
        sku: str,
        metadata: Metadata = None,
    ) -> warehouse_pb2.Item:
        stub = self._stub_for(sku)
        request = warehouse_pb2.QueryItemRequest(sku=sku)
        response = stub.QueryItem(
            request,
            timeout=self._timeout,
            metadata=metadata,
        )
        return response.item

    @property
    def endpoints(self) -> Sequence[str]:
        return tuple(self._endpoints)
