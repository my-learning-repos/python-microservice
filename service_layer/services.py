from __future__ import annotations

from datetime import date
from typing import Optional

from domain import model
from domain.model import OrderLine
from adapters.repository import AbstractRepository


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(
        ref: str,
        sku: str,
        quantity: int,
        eta: Optional[date],
        repo: AbstractRepository,
        session
) -> None:
    repo.add(model.Batch(ref, sku, quantity, eta))
    session.commit()


def allocate(
        order_id: int,
        sku: str,
        quantity: int,
        repo: AbstractRepository,
        session
) -> str:
    line = model.OrderLine(order_id, sku, quantity)
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f"Invalid sku {line.sku}")
    batchref = model.allocate(line, batches)
    session.commit()
    return batchref
