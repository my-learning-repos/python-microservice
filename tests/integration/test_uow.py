import threading
import time
import traceback

import pytest
from allocation.domain import model
from allocation.service_layer import unit_of_work
from tests.e2e.test_api import random_sku, random_batchref, random_order_id


def insert_batch(session, ref, sku, qty, eta, product_version=1):
    session.execute(
        "INSERT INTO products (sku, version_number) VALUES (:sku, :version)",
        dict(sku=sku, version=product_version),
    )
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
        " VALUES (:ref, :sku, :qty, :eta)",
        dict(ref=ref, sku=sku, qty=qty, eta=eta),
    )


def get_allocated_batch_ref(session, orderid, sku):
    [[orderlineid]] = session.execute(
        "SELECT id FROM order_lines WHERE order_id=:orderid AND sku=:sku",
        dict(orderid=orderid, sku=sku),
    )
    [[batchref]] = session.execute(
        "SELECT b.reference FROM allocations JOIN batches AS b ON batch_id = b.id"
        " WHERE order_line_id=:orderlineid",
        dict(orderlineid=orderlineid),
    )
    return batchref


def test_uow_can_retrieve_a_batch_and_allocate_to_it(session_factory):
    session = session_factory()
    insert_batch(session, "batch1", "HIPSTER-WORKBENCH", 100, None)
    session.commit()

    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    with uow:
        product = uow.products.get(sku="HIPSTER-WORKBENCH")
        line = model.OrderLine("o1", "HIPSTER-WORKBENCH", 10)
        product.allocate(line)
        uow.commit()

    batchref = get_allocated_batch_ref(session, "o1", "HIPSTER-WORKBENCH")

    assert batchref == "batch1"


def test_rolls_back_uncommitted_work_by_default(session_factory):
    session = session_factory()
    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    with uow:
        insert_batch(uow.session, "batch1", "MEDIUM-PLINTH", 100, None)

    rows = list(session.execute("SELECT * FROM batches"))

    assert rows == []


def test_rolls_back_on_error(session_factory):
    class MyException(Exception):
        pass

    uow = unit_of_work.SqlAlchemyUnitOfWork(session_factory)
    with pytest.raises(MyException):
        with uow:
            insert_batch(uow.session, "batch", "LARGE-FORK", 100, None)
            raise MyException()

    session = session_factory()
    rows = list(session.execute("SELECT * FROM batches"))

    assert rows == []


def try_to_allocate(order_id, sku, exceptions):
    line = model.OrderLine(order_id, sku, 10)
    try:
        with unit_of_work.SqlAlchemyUnitOfWork() as uow:
            product = uow.products.get(sku=sku)
            product.allocate(line)
            time.sleep(0.2)
            uow.commit()
    except Exception as e:
        print(traceback.format_exc())
        exceptions.append(e)


def test_concurrent_updates_to_version_are_not_allowed(postgres_session_factory):
    sku, batch = random_sku(), random_batchref()
    session = postgres_session_factory()
    insert_batch(session, batch, sku, 100, eta=None, product_version=1)
    session.commit()

    order1, order2 = random_order_id(1), random_order_id(2)
    exceptions = list()

    try_to_allocate_order_1 = lambda: try_to_allocate(order1, sku, exceptions)
    try_to_allocate_order_2 = lambda: try_to_allocate(order2, sku, exceptions)

    thread1 = threading.Thread(target=try_to_allocate_order_1)
    thread2 = threading.Thread(target=try_to_allocate_order_2)

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    [[version]] = session.execute(
        "SELECT version_number FROM products WHERE sku=:sku",
        dict(sku=sku)
    )

    assert version == 2
    exception = [exceptions]

    assert "could not serialize access due to concurrent update" in str(
        exception)

    orders = session.execute(
        "SELECT order_id FROM allocations"
        " JOIN batches ON allocations.batch_id = batches.id"
        " JOIN order_lines ON allocations.order_line_id=order_lines.id"
        " WHERE order_lines.sku=:sku",
        dict(sku=sku)
    )

    assert orders.rowcount ==1
    with unit_of_work.SqlAlchemyUnitOfWork() as uow:
        uow.session.execute("select 1")
