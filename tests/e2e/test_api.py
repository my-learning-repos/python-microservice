import uuid
import pytest
import requests

from allocation import config
from ..random_refs import random_sku, random_batchref, random_order_id
from . import api_client


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
def test_happy_path_returns_202_and_allocated_batch(add_stock):
    order_id = random_order_id()
    sku = random_sku()
    other_sku = random_sku("other")
    early_batch = random_batchref(1)
    later_batch = random_batchref(2)
    other_batch = random_batchref(3)

    api_client.post_to_add_batch(later_batch, sku, 100, "2011-01-02"),
    api_client.post_to_add_batch(early_batch, sku, 100, "2011-01-01"),
    api_client.post_to_add_batch(later_batch, other_sku, 100, None),

    url = config.get_api_url()

    r = api_client.post_to_allocate(order_id, sku, quantity=3)

    assert r.status_code == 202

    r = api_client.get_allocation(order_id)

    assert r.json() == [
        {"sku":sku, "batchref":early_batch}
    ]


@pytest.mark.usefixtures("restart_api")
def test_unhappy_path_returns_400_and_error_message():
    unknow_sku, order_id = random_sku(), random_order_id()
    r = api_client.post_to_allocate(order_id, unknow_sku, 20, expect_success=False)

    assert r.status_code ==400
    assert r.json()["message"] == f"Invalid sku {unknow_sku}"

    r = api_client.get_allocation(order_id)
    assert r.status_code ==404
