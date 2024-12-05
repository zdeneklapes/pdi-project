build:
	docker compose build

down:
	docker compose down

fish:
	docker compose exec app fish

run:
	docker compose run --remove-orphans --entrypoint /usr/bin/fish app

lint:
	docker compose run --rm app  black --line-length 120 src/

restart: down up
