from dataclasses import dataclass


class Event:
    pass


@dataclass
class OutOfStock(Event):
    sku: str


@dataclass
class Allocated(Event):
    order_id: str
    sku: str
    quantity: int
    batchref: str


@dataclass
class Deallocated(Event):
    order_id: str
    sku: str
    quantity: int
