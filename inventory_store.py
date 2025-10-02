"""Thread-safe inventory store used by the logistics gRPC service."""

from __future__ import annotations

import threading
from typing import Dict, Optional

from warehouse_pb2 import Item


class InventoryError(Exception):
    """Base class for store errors."""


class ItemAlreadyExistsError(InventoryError):
    """Raised when attempting to create an item that already exists."""


class ItemNotFoundError(InventoryError):
    """Raised when an operation references a missing SKU."""


class InsufficientQuantityError(InventoryError):
    """Raised when a take operation would drop quantity below zero."""


def _clone(item: Item) -> Item:
    """Return a deep copy of an Item message."""
    clone = Item()
    clone.CopyFrom(item)
    return clone


class InventoryStore:
    """In-memory inventory store with coarse-grained locking."""

    def __init__(self) -> None:
        self._items: Dict[str, Item] = {}
        self._lock = threading.RLock()

    def add_item(self, item: Item) -> Item:
        sku = item.sku.strip()
        if not sku:
            raise ValueError("SKU must not be empty")
        if item.quantity < 0:
            raise ValueError("Quantity must be non-negative")

        with self._lock:
            if sku in self._items:
                raise ItemAlreadyExistsError(f"Item {sku} already exists")
            stored = _clone(item)
            stored.sku = sku
            self._items[sku] = stored
            return _clone(stored)

    def update_item(
        self,
        sku: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        quantity: Optional[int] = None,
    ) -> Item:
        sku = sku.strip()
        if not sku:
            raise ValueError("SKU must not be empty")

        with self._lock:
            if sku not in self._items:
                raise ItemNotFoundError(f"Item {sku} not found")

            item = self._items[sku]
            if name is not None:
                item.name = name
            if description is not None:
                item.description = description
            if quantity is not None:
                if quantity < 0:
                    raise ValueError("Quantity must be non-negative")
                item.quantity = quantity

            return _clone(item)

    def take_item(self, sku: str, amount: int) -> Item:
        sku = sku.strip()
        if not sku:
            raise ValueError("SKU must not be empty")
        if amount <= 0:
            raise ValueError("Take amount must be positive")

        with self._lock:
            if sku not in self._items:
                raise ItemNotFoundError(f"Item {sku} not found")

            item = self._items[sku]
            if item.quantity < amount:
                raise InsufficientQuantityError(
                    f"Item {sku} has insufficient quantity ({item.quantity} < {amount})"
                )

            item.quantity -= amount
            return _clone(item)

    def query_item(self, sku: str) -> Item:
        sku = sku.strip()
        if not sku:
            raise ValueError("SKU must not be empty")

        with self._lock:
            if sku not in self._items:
                raise ItemNotFoundError(f"Item {sku} not found")
            return _clone(self._items[sku])

    def clear(self) -> None:
        with self._lock:
            self._items.clear()
