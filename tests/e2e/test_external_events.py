import json
import pytest
from tenacity import Retrying, RetryError, stop_after_delay
from . import api_client, redis_client
from ..random_refs import random_batchref, random_order_id, random_sku

@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_change_batch_quantity_leading_to_reallocation():
    order_id, sku = random_order_id(), random_sku()
    earlier_batch, later_batch = random_batchref("old"), random_batchref("newer")

    api_client.post_to_add_batch(earlier_batch, sku, quantity=10, eta="2011-01-01")
    api_client.post_to_add_batch(later_batch, sku, quantity=10, eta="2011-01-02")

    r = api_client.post_to_allocate(order_id, sku, 10)
    assert  r.ok
    response = api_client.get_allocation(order_id)

    assert response.json()[0]["batchref"] == earlier_batch

    subscription = redis_client.subscribe_to("line_allocated")

    redis_client.publish_message(
        "change_batch_quantity",
        {"batchref":earlier_batch, "quantity":5},
    )

    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            if message:
                messages.append(message)

            data = json.loads(messages[-1]["data"])
            assert data["order_id"] == order_id
            assert data["batchref"] == later_batch