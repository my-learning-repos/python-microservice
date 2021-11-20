from datetime import datetime

from flask import Flask, request, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from allocation import config
from allocation.domain import commands
from allocation.adapters import repository
from allocation.service_layer import unit_of_work
from allocation.service_layer.handlers import InvalidSku
from allocation import views, bootstrap

get_session = sessionmaker(bind=create_engine(config.get_postgres_uri()))

app = Flask(__name__)
bus = bootstrap.bootstrap()


@app.route("/allocate", methods=["POST"])
def allocate_endpoint():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)

    try:
        command = commands.Allocate(request.json["order_id"],
                                  request.json["sku"],
                                  request.json["quantity"]
                                  )
        bus.handle(command)
    except InvalidSku as e:
        return {"message": str(e)}, 400
    return "ok", 202


@app.route("/add_batch", methods=["POST"])
def add_batch():
    session = get_session()
    repo = repository.SqlAlchemyRepository(session)
    eta = request.json["eta"]
    if eta is not None:
        eta = datetime.fromisoformat(eta).date()
    command = commands.CreateBatch(
        request.json["ref"],
        request.json["sku"],
        request.json["quantity"],
        eta,
    )
    bus.handle(command)
    return "OK", 201


@app.route("/allocations/<order_id>", methods=["GET"])
def allocations_view_endpoint(order_id):
    uow = unit_of_work.SqlAlchemyUnitOfWork()
    result = views.allocations(order_id, uow)
    if not result:
        return "not found", 404
    return jsonify(result), 200
