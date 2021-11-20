from __future__ import annotations
from typing import TYPE_CHECKING, Union, List, Dict, Type, Callable
from tenacity import Retrying, RetryError, stop_after_attempt, wait_exponential
from allocation.domain import events, commands

if TYPE_CHECKING:
    from . import unit_of_work

Message = Union[commands.Command, events.Event]


class MessageBus:
    def __init__(self,
                 uow: unit_of_work.AbstractUnitOfWork,
                 event_handlers: Dict[Type[events.Event], List[Callable]],
                 command_handlers: Dict[
                     Type[commands.Command], Callable],
                 ):
        self.uow = uow
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers
        self.queue = [None]

    def handle(self, message: Message):
        results = []
        self.queue = [message]
        while self.queue:
            message = self.queue.pop(0)
            if isinstance(message, events.Event):
                self.handle_event(message)
            elif isinstance(message, commands.Command):
                cmd_result = self.handle_command(message)
                results.append(cmd_result)
            else:
                raise Exception(f"{message} was not an Event or Command")
        return results

    def handle_event(self, event: events.Event):
        for handler in self.event_handlers[type(event)]:
            try:
                for attempt in Retrying(
                        stop=stop_after_attempt(3),
                        wait=wait_exponential()
                ):
                    with attempt:
                        handler(event)
                        self.queue.extend(self.uow.collect_new_events())
            except RetryError as retry_failure:
                continue

    def handle_command(self, command: commands.Command):
        try:
            handler = self.command_handlers[type(command)]
            result = handler(command)
            self.queue.extend(self.uow.collect_new_events())
            return result
        except Exception:
            raise
