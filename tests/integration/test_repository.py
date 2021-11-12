from allocation.domain import model
from allocation.adapters import repository


def test_repository_can_save_a_batch(session):
    batch = model.Batch("batch1", "RUSTY-SOAPDISH", 100, eta=None)

    repo = repository.SqlAlchemyRepository(session)
    repo.add(batch)

    session.commit()

    rows = session.execute(
        'SELECT reference, sku, _purchased_quantity, eta FROM "batches"'
    )
    assert list(rows) == [("batch1", "RUSTY-SOAPDISH", 100, None)]


def insert_order_line(session):
    session.execute(
        "INSERT INTO order_lines (order_id, sku, quantity)"
        'VALUES ("order1", "GENERIC-SOFA",12)'
    )
    [[order_line_id]] = session.execute(
        "SELECT id FROM order_lines WHERE order_id=:order_id AND sku=:sku",
        dict(order_id="order1", sku="GENERIC-SOFA"),
    )
    return order_line_id


def insert_batch(session, batch_id):
    session.execute(
        "INSERT OR IGNORE INTO products (sku)"
        ' VALUES ("GENERIC-SOFA")',
    )
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
        ' VALUES (:batch_id, "GENERIC-SOFA", 100, null)',
        dict(batch_id=batch_id),
    )
    [[batch_id]] = session.execute(
        'SELECT id FROM batches WHERE reference=:batch_id AND sku="GENERIC-SOFA"',
        dict(batch_id=batch_id),
    )
    return batch_id


def insert_allocation(session, order_line_id, batch_id):
    session.execute(
        "INSERT INTO allocations (order_line_id, batch_id)"
        " VALUES (:order_line_id, :batch_id)",
        dict(order_line_id=order_line_id, batch_id=batch_id),
    )


def test_repository_can_retrieve_a_batch_with_allocations(session):
    order_line_id = insert_order_line(session)
    batch_1_id = insert_batch(session, "batch1")

    insert_batch(session, "batch2")

    insert_allocation(session, order_line_id, batch_1_id)

    repo = repository.SqlAlchemyRepository(session)
    retrieved = repo.get("GENERIC-SOFA").batches[0]

    expected = model.Batch("batch1", "GENERIC-SOFA", 100, eta=None)
    assert retrieved == expected
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12),
    }


def get_allocations(session, batch_id):
    rows = list(
        session.execute(
            "SELECT order_id FROM allocations"
            " JOIN order_lines ON allocations.order_line_id = order_lines.id"
            " JOIN batches ON allocations.batch_id = batches.id"
            " WHERE batches.reference = :batch_id",
            dict(batch_id=batch_id)
        )
    )
    return {row[0] for row in rows}


def test_updating_a_batch(session):
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)

    batch.allocate(order1)
    repo = repository.SqlAlchemyRepository(session)
    repo.add(batch)
    session.commit()

    batch.allocate(order2)
    repo.add(batch)
    session.commit()

    assert get_allocations(session, "batch1") == {"order1", "order2"}
