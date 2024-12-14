# Docker
build:
	docker compose build

down:
	docker compose down

fish:
	docker compose exec app fish

run:
	docker compose run --remove-orphans --entrypoint /usr/bin/fish app

lint:
	docker compose run --rm app uv run black --line-length 120 src/

restart: down up

# env
install:
	uv sync

# Cluster
start-cluster:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/start-cluster.sh
stop-cluster:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/stop-cluster.sh

# Testing on cluster
run-cluster-1:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/flink run --python /opt/app/src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-1 --output_dir tmp/results/test-task-1 --mode stream --task 1
run-cluster-2:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/flink run --python /opt/app/src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-2 --output_dir tmp/results/test-task-2 --mode stream --task 2
run-cluster-3:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/flink run --python /opt/app/src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-3 --output_dir tmp/results/test-task-3 --mode stream --task 3
run-cluster-4:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/flink run --python /opt/app/src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-4 --output_dir tmp/results/test-task-4 --mode stream --task 4
run-cluster-5:
	/opt/app/.venv/lib/python3.11/site-packages/pyflink/bin/flink run --python /opt/app/src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-5 --output_dir tmp/results/test-task-5 --mode stream --task 5

# Testing on local
run-1:
	python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-1 --output_dir tmp/results/test-task-1 --mode stream --task 1
run-2:
	python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-2 --output_dir tmp/results/test-task-2 --mode stream --task 2
run-3:
	python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-3 --output_dir tmp/results/test-task-3 --mode stream --task 3
run-4:
	python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-4 --output_dir tmp/results/test-task-4 --mode stream --task 4
run-5:
	python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/test-task-5 --output_dir tmp/results/test-task-5 --mode stream --task 5


# ------
pack:
	zip xlapes02.zip -r src/ Makefile *.md pyproject.toml .python-version uv.lock Dockerfile docker-compose.yml
