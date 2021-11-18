from __future__ import annotations

from typing import TYPE_CHECKING

from allocation.adapters import email, redis_eventpublisher
from allocation.domain import events, commands

from allocation.domain import model

if TYPE_CHECKING:
    from . import unit_of_work


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(cmd: commands.CreateBatch, uow: unit_of_work.AbstractUnitOfWork
              ) -> None:
    with uow:
        product = uow.products.get(sku=cmd.sku)
        if product is None:
            product = model.Product(cmd.sku, batches=[])
            uow.products.add(product)
        product.batches.append(
            model.Batch(cmd.ref, cmd.sku, cmd.quantity, cmd.eta))
        uow.commit()


def allocate(
        cmd: commands.Allocate, uow: unit_of_work.AbstractUnitOfWork
) -> str:
    line = model.OrderLine(cmd.order_id, cmd.sku, cmd.quantity)
    with uow:
        product = uow.products.get(sku=line.sku)
        if product is None:
            raise InvalidSku(f"Invalid sku {line.sku}")
        batchref = product.allocate(line)
        uow.commit()
    return batchref


def change_batch_quantity(
        cmd: commands.ChangeBatchQuantity,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        product = uow.products.get_by_batchref(cmd.ref)
        product.change_batch_quantity(ref=cmd.ref, quantity=cmd.quantity)
        uow.commit()


def send_out_of_stock_notification(
        event: events.OutOfStock,
        uow: unit_of_work.AbstractUnitOfWork
):
    email.send_mail(
        "stock@made.com",
        f"Out of stock for {event.sku}"
    )


def publish_allocated_event(
        event: events.Allocated,
        uow: unit_of_work.AbstractUnitOfWork
):
    redis_eventpublisher.publish("line_allocated", event)
