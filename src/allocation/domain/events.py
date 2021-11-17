from dataclasses import dataclass
from datetime import date
from typing import Optional


class Event:
    pass


@dataclass
class BatchCreated(Event):
    ref: str
    sku: str
    quantity: int
    eta: Optional[date] = None


@dataclass
class BatchQuantityChanged(Event):
    ref: str
    quantity: int


@dataclass
class AllocationRequired(Event):
    order_id: str
    sku: str
    quantity: int


@dataclass
class OutOfStock(Event):
    sku: str
