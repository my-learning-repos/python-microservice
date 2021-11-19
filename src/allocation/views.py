from allocation.service_layer import unit_of_work


def allocations(order_id:int, uow: unit_of_work.SqlAlchemyUnitOfWork):
    with uow:
        results = uow.session.execute(
            """
            SELECT sku, batchref FROM allocations_view WHERE orderid = :order_id
            """,
            dict(order_id=order_id),
        )
        return [dict(r) for r in results]