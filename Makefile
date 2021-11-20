export COMPOSE_DOCKER_CLI_BUILD=1
export DOCKER_BUILDKIT=1

all: down build up test

up:
	docker-compose up -d

build:
	docker-compose build

down:
	docker-compose down --remove-orphans

test: up
	docker-compose run --rm --no-deps --entrypoint=pytest api -s /tests/unit /tests/integration /tests/e2e