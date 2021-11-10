from __future__ import annotations

from datetime import date
from typing import Optional

from domain import model
from domain.model import OrderLine
from adapters.repository import AbstractRepository
from service_layer import unit_of_work


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(
        ref: str,
        sku: str,
        quantity: int,
        eta: Optional[date],
        uow: unit_of_work.AbstractUnitOfWork,

) -> None:
    with uow:
        uow.batches.add(model.Batch(ref, sku, quantity, eta))
        uow.commit()


def allocate(
        order_id: int,
        sku: str,
        quantity: int,
        uow: unit_of_work.AbstractUnitOfWork,

) -> str:
    line = model.OrderLine(order_id, sku, quantity)
    with uow:
        batches = uow.batches.list()
        if not is_valid_sku(line.sku, batches):
            raise InvalidSku(f"Invalid sku {line.sku}")
        batchref = model.allocate(line, batches)
        uow.commit()
    return batchref
