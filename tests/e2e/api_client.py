import requests
from allocation import config


def post_to_add_batch(ref, sku, quantity, eta):
    r = requests.post(
        f"{config.get_api_url()}/add_batch", json={
            "ref": ref, "sku": sku, "quantity": quantity, "eta": eta
        }
    )
    assert r.status_code == 201


def post_to_allocate(order_id, sku, quantity, expect_success=True):
    r = requests.post(
        f"{config.get_api_url()}/allocate", json={
            "order_id": order_id, "sku": sku, "quantity": quantity})
    if expect_success:
        assert r.status_code == 201

    return r
