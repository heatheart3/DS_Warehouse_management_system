from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import grpc
import pytest

from distributed_inventory import DistributedInventoryClient
from inventory_server import InventoryService
from inventory_store import InventoryStore


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode, details: str) -> None:
        super().__init__()
        self._code = code
        self._details = details

    def code(self) -> grpc.StatusCode:  # type: ignore[override]
        return self._code

    def details(self) -> str:  # type: ignore[override]
        return self._details


class _FakeContext:
    def __init__(self, metadata: Optional[Iterable[Tuple[str, str]]]) -> None:
        self._metadata = list(metadata or [])

    def invocation_metadata(self) -> Sequence[Tuple[str, str]]:
        return list(self._metadata)

    def abort(self, code: grpc.StatusCode, details: str):
        raise _FakeRpcError(code, details)


class _LocalStub:
    def __init__(self, service: InventoryService) -> None:
        self._service = service

    def AddItem(self, request, timeout=None, metadata=None):  # noqa: N802
        context = _FakeContext(metadata)
        return self._service.AddItem(request, context)

    def UpdateItem(self, request, timeout=None, metadata=None):  # noqa: N802
        context = _FakeContext(metadata)
        return self._service.UpdateItem(request, context)

    def TakeItem(self, request, timeout=None, metadata=None):  # noqa: N802
        context = _FakeContext(metadata)
        return self._service.TakeItem(request, context)

    def QueryItem(self, request, timeout=None, metadata=None):  # noqa: N802
        context = _FakeContext(metadata)
        return self._service.QueryItem(request, context)


@pytest.fixture
def grpc_cluster() -> Tuple[List[str], Dict[str, InventoryService]]:
    endpoints = ["endpoint-a", "endpoint-b"]
    services: Dict[str, InventoryService] = {}
    for endpoint in endpoints:
        services[endpoint] = InventoryService(store=InventoryStore())
    return endpoints, services


@pytest.fixture
def client(grpc_cluster):
    endpoints, services = grpc_cluster

    def stub_factory(endpoint: str):
        service = services[endpoint]
        return _LocalStub(service), None  # type: ignore[return-value]

    with DistributedInventoryClient(endpoints, stub_factory=stub_factory) as client:
        yield client


def test_add_query_update_take_flow(client):
    sku = "SKU-1001"
    client.add_item(sku, name="Widget", description="Standard widget", quantity=250)

    item = client.query_item(sku)
    assert item.sku == sku
    assert item.quantity == 250

    updated = client.update_item(
        sku,
        name="Widget Pro",
        description="Upgraded widget",
        quantity=300,
    )
    assert updated.name == "Widget Pro"
    assert updated.quantity == 300

    after_take = client.take_item(sku, amount=120)
    assert after_take.quantity == 180


def test_hash_distribution_across_nodes(client):
    endpoints_seen = set()
    for idx in range(32):
        sku = f"SKU-{idx:04d}"
        endpoints_seen.add(client.endpoint_for_sku(sku))
        client.add_item(
            sku,
            name=f"Item {idx}",
            description="Test item",
            quantity=idx + 1,
        )

    assert set(client.endpoints) == endpoints_seen

    sample_sku = "SKU-0015"
    before = client.query_item(sample_sku)
    assert before.quantity == 16
    after = client.take_item(sample_sku, amount=6)
    assert after.quantity == 10


def test_take_item_raises_on_insufficient_inventory(client):
    client.add_item("SKU-LIMIT", name="Limited", description="", quantity=5)

    with pytest.raises(grpc.RpcError) as exc_info:
        client.take_item("SKU-LIMIT", amount=10)
    assert exc_info.value.code() == grpc.StatusCode.FAILED_PRECONDITION
