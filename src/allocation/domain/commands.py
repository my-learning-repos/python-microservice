from datetime import date
from typing import Optional
from dataclasses import dataclass


class Command:
    pass


@dataclass
class Allocate(Command):
    order_id: str
    sku: str
    quantity: int


@dataclass
class CreateBatch(Command):
    ref: str
    sku: str
    quantity: int
    eta: Optional[date] = None


@dataclass
class ChangeBatchQuantity(Command):
    ref: str
    quantity: int
