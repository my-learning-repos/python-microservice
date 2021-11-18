

all: down build up test

up:
	docker-compose up -d

build:
	docker-compose build

down:
	docker-compose down --remove-orphans

test: up
	docker-compose run --rm --no-deps --entrypoint=pytest api /tests/unit /tests/integration /tests/e2e