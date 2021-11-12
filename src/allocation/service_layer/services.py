from __future__ import annotations

from datetime import date
from typing import Optional

from allocation.domain import model
from allocation.service_layer import unit_of_work


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
        product = uow.products.get(sku=sku)
        if product is None:
            product = model.Product(sku, batches=[])
            uow.products.add(product)
        product.batches.append(model.Batch(ref, sku, quantity, eta))
        uow.commit()


def allocate(
        order_id: int,
        sku: str,
        quantity: int,
        uow: unit_of_work.AbstractUnitOfWork,

) -> str:
    line = model.OrderLine(order_id, sku, quantity)
    with uow:
        product = uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        batchref = product.allocate(line)
        uow.commit()
    return batchref
