from __future__ import annotations

from datetime import date
from typing import Optional, TYPE_CHECKING

from allocation.adapters import email
from allocation.domain import events

from allocation.domain import model

if TYPE_CHECKING:
    from . import unit_of_work

class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(event: events.BatchCreated, uow: unit_of_work.AbstractUnitOfWork
              ) -> None:
    with uow:
        product = uow.products.get(sku=event.sku)
        if product is None:
            product = model.Product(event.sku, batches=[])
            uow.products.add(product)
        product.batches.append(
            model.Batch(event.ref, event.sku, event.quantity, event.eta))
        uow.commit()


def allocate(
        event: events.AllocationRequired, uow: unit_of_work.AbstractUnitOfWork
) -> str:
    line = model.OrderLine(event.order_id, event.sku, event.quantity)
    with uow:
        product = uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        batchref = product.allocate(line)
        uow.commit()
    return batchref


def change_batch_quantity(
        event: events.BatchQuantityChanged,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        product = uow.products.get_by_batchref(event.ref)
        product.change_batch_quantity(ref=event.ref, quantity=event.quantity)
        uow.commit()


def send_out_of_stock_notification(
        event: events.OutOfStock,
        uow: unit_of_work.AbstractUnitOfWork
):
    email.send_mail(
        "stock@made.com",
        f"Out of stock for {event.sku}"
    )


