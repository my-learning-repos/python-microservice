from allocation.domain import model
from datetime import date


def test_order_line_mapper_can_load_lines(session):
    session.execute(
        "INSERT INTO order_lines (order_id , sku, quantity) VALUES "
        '("order1", "RED-CHAIR", 12),'
        '("order1", "RED-TABLE", 13),'
        '("order2", "BLUE-LIPSTICK", 14)'
    )
    expected = [
        model.OrderLine("order1", "RED-CHAIR", 12),
        model.OrderLine("order1", "RED-TABLE", 13),
        model.OrderLine("order2", "BLUE-LIPSTICK", 14),
    ]
    assert session.query(model.OrderLine).all() == expected


def test_order_line_mapper_can_save_lines(session):
    new_line = model.OrderLine("order1", "DECORATIVE-WIDGET", 12)
    session.add(new_line)
    session.commit()

    rows = list(session.execute('SELECT order_id, sku, quantity FROM "order_lines"'))
    assert rows == [("order1", "DECORATIVE-WIDGET",12)]


def test_retrieving_batches(session):
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
        ' VALUES ("batch1", "sku1", 100, null),'
        ' ("batch2", "sku2", 200, "2011-04-11")'
    )
    expected = [
        model.Batch("batch1", "sku1", 100, eta=None),
        model.Batch("batch2", "sku2", 200, eta=date(2011, 4, 11)),
    ]

    assert session.query(model.Batch).all() == expected


def test_saving_batches(session):
    batch = model.Batch("batch1", "sku1", 100, eta=None)
    session.add(batch)
    session.commit()
    rows = session.execute(
        'SELECT reference, sku, _purchased_quantity, eta FROM "batches"'
    )

    assert list(rows) == [("batch1", "sku1", 100, None)]


def test_saving_allocations(session):
    line = model.OrderLine("order1", "sku1", 12)
    batch = model.Batch("batch1", "sku1", 100, eta=None)
    batch.allocate(line)
    session.add(batch)
    session.commit()
    rows = list(session.execute('SELECT order_line_id, batch_id FROM "allocations"'))
    assert rows == [(batch.id, line.id)]


def test_retrieving_allocations(session):
    session.execute(
        'INSERT INTO order_lines (order_id, sku, quantity) VALUES ("order1", "sku1", 12)'
    )
    [[order_line_id]] = session.execute(
        "SELECT id FROM order_lines WHERE order_id=:order_id AND sku=:sku",
        dict(order_id="order1", sku="sku1")
    )
    session.execute(
        "INSERT INTO batches (reference, sku, _purchased_quantity, eta)"
        ' VALUES ("batch1", "sku1", 100, null)'
    )

    [[batch_id]] = session.execute(
        "SELECT id FROM batches WHERE reference=:ref AND sku=:sku",
        dict(ref="batch1", sku="sku1")
    )
    session.execute(
        "INSERT INTO allocations (order_line_id, batch_id) VALUES (:order_line_id, :batch_id)",
        dict(order_line_id=order_line_id, batch_id=batch_id)
    )

    batch = session.query(model.Batch).first()

    assert batch._allocations == {model.OrderLine("order1", "sku1", 12)}
