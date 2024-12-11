# INSTALL

## Project PDI - Process Real-Time Public Transit Data Using Apache Spark/Flink

### Author: Zdenek Lapes, xlapes02

---

## Requirements

- Docker
- Docker-compose

## Install dependencies

```sh
# Build the project
make build

# Go inside the container
make run

# Install dependencies inside the container
make install

# Go to virtual environment
source venv/bin/activate.fish
```

## Running the project the EASY WAY

### Local run inside docker

```sh
# Run the project locally
make run-1
make run-2
make run-3
make run-4
make run-5
```

### Run on the cluster inside docker

```sh
# Run the project on the cluster
make start-cluster
make cluster-run-1
make cluster-run-2
make cluster-run-3
make cluster-run-4
make cluster-run-5
make stop-cluster
```

## Running the project the HARD WAY

### Program parameters

```
python3 main.py --help
```

### Local run inside docker

```sh
# Run the data downloading infinitely (no processing)
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/1 --output_dir tmp/results/1 --mode download --task 1

# Run the task 1 in the batch mode, and will be processing the data it found under the data_dir only once
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/1 --output_dir tmp/results/1 --mode batch --task 1

# Run the task 1 in the stream mode, and will be periodically downloading and processing the data in the data_dir infinitely
python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/1 --output_dir tmp/results/1 --mode stream --task 1
```

### Run on the cluster inside docker

```sh
# Start the cluster
.venv/lib/python3.11/site-packages/pyflink/bin/start-cluster.sh

# Run the task 1 in the stream mode, and will be periodically downloading and processing the data in the data_dir infinitely
.venv/lib/python3.11/site-packages/pyflink/bin/flink run --python src/main.py --bounding-box -180 180 -180 180 --data_dir tmp/data/4 --output_dir tmp/results/4 --mode stream --task 1

# Stop the cluster
.venv/lib/python3.11/site-packages/pyflink/bin/stop-cluster.sh
```
