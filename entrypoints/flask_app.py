from datetime import datetime

from flask import Flask, request
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import config
from domain import model
from adapters import orm, repository
from service_layer import services, unit_of_work

orm.start_mappers()
get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))

app = Flask(__name__)


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)

    try:
        batchref = services.allocate(request.json["order_id"],
                                     request.json["sku"],
                                     request.json["quantity"],
                                     unit_of_work.SqlAlchemyUnitOfWork(),
                                     )
    except (model.OutOfStock, services.InvalidSku) as e:
        return {"message": str(e)}, 400

    return {"batchref": batchref}, 201


@app.route("/add_batch", methods=["POST"])
def add_batch():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    eta = request.json["eta"]
    if eta is not  None:
        eta = datetime.fromisoformat(eta).date()
    services.add_batch(
        request.json["ref"],
        request.json["sku"],
        request.json["quantity"],
        eta,
        unit_of_work.SqlAlchemyUnitOfWork()
    )
    return "OK", 201
