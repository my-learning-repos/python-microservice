from __future__ import annotations
from typing import TYPE_CHECKING, Union, List

from allocation.domain import events, commands
from . import handlers

if TYPE_CHECKING:
    from . import unit_of_work

Message = Union[commands.Command, events.Event]


def handle(message: Message, uow: unit_of_work.AbstractUnitOfWork):
    results = []
    queue = [message]
    while queue:
        message = queue.pop(0)
        if isinstance(message, events.Event):
            handle_event(message, queue, uow)
        elif isinstance(message, commands.Command):
            cmd_result = handle_command(message, queue, uow)
            results.append(cmd_result)
        else:
            raise Exception(f"{message} was not an Event or Command")
    return results


def handle_event(event: events.Event, queue: List[Message],
                 uow: unit_of_work.AbstractUnitOfWork):
    for handler in EVENT_HANDLERS[type(event)]:
        try:
            handler(event, uow=uow)
            queue.extend(uow.collect_new_events())
        except Exception:
            continue


def handle_command(command: commands.Command, queue: List[Message],
                   uow: unit_of_work.AbstractUnitOfWork):
    try:
        handler = COMMAND_HANDLERS[type(command)]
        result = handler(command, uow=uow)
        queue.extend(uow.collect_new_events())
        return result
    except Exception:
        raise


EVENT_HANDLERS = {
    events.OutOfStock: [handlers.send_out_of_stock_notification],
}
COMMAND_HANDLERS = {
    commands.CreateBatch: handlers.add_batch,
    commands.ChangeBatchQuantity: handlers.change_batch_quantity,
    commands.Allocate: handlers.allocate,
}
