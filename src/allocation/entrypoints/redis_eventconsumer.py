import json
import redis
from allocation import config, bootstrap
from allocation.domain import commands

r = redis.Redis(**config.get_redis_host_and_port())


def main():
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("change_batch_quantity")
    bus = bootstrap.bootstrap()

    for m in pubsub.listen():
        handle_change_batch_quantity(m, bus)


def handle_change_batch_quantity(m, bus):
    data = json.loads(m["data"])
    cmd = commands.ChangeBatchQuantity(ref=data["batchref"],
                                       quantity=data["quantity"])
    bus.handle(cmd)


if __name__ == "__main__":
    main()
