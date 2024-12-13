import datetime
import json
import os
import random
from argparse import ArgumentParser
from pathlib import Path

from lib.program import Program


def setup_environment():
    """Set up directories and environment variables for testing."""
    program = Program({}, None)
    base_dir = program.ROOT_DIR / "tmp" / "testing"
    data_dir = base_dir / "data"
    output_dir = base_dir / "output"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return {
        "base_dir": base_dir,
        "data_dir": data_dir,
        "output_dir": output_dir,
    }


def create_data(test_data: list, output_dir: Path):
    if output_dir.exists():
        answer = input(f"Directory {output_dir} already exists. Do you want to delete it? [y/N] ")
        if answer.lower() == "y":
            print(f"Removing directory {output_dir}")
            os.system(f"rm -rf {output_dir}")
            output_dir.mkdir(parents=True, exist_ok=True)
    else:
        print(f"Creating directory {output_dir}")
        output_dir.mkdir(parents=True, exist_ok=True)
    for idx, record in enumerate(test_data):
        filename = datetime.datetime.now().strftime("%Y_%m_%d_%H-%M-%S-%f")
        file_path = Path(output_dir) / f"{filename}.json"
        print(f"Writing test data to {file_path}")
        with open(file_path, "w") as f:
            json.dump(record, f)


def test_data_task_1(setup_environment):
    """Create deterministic mock data for testing all tasks."""
    # Set a fixed random seed for reproducibility
    random.seed(42)
    test_data = []
    for i in range(10):
        record = {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": round(random.uniform(16.5, 16.8), 6),
                "y": round(random.uniform(49.1, 49.3), 6),
            },
            "attributes": {
                "id": str(random.randint(1000, 1003)),
                "vtype": random.randint(0, 5),
                "ltype": random.randint(0, 5),
                "bearing": round(random.uniform(-1.0, 360.0), 1),
                "lineid": random.randint(1, 200),
                "linename": f"Line_{random.randint(1, 200)}",
                "routeid": random.randint(1, 5000),
                "course": str(random.randint(10000, 99999)),
                "lf": random.choice(["true", "false"]),
                "delay": round(random.uniform(0, 300), 1),
                "laststopid": random.randint(1000, 1005),
                "finalstopid": random.randint(1000, 1005),
                "isinactive": "false",
                "lastupdate": random.randint(1700000000000, 1734046845201),
                "globalid": f"{{{''.join(random.choices('ABCDEF1234567890', k=8))}-"
                            f"{''.join(random.choices('ABCDEF1234567890', k=4))}-"
                            f"{''.join(random.choices('ABCDEF1234567890', k=4))}-"
                            f"{''.join(random.choices('ABCDEF1234567890', k=4))}-"
                            f"{''.join(random.choices('ABCDEF1234567890', k=12))}}}",
            },
        }
        test_data.append(record)

    # Save test data to JSON files
    create_data(test_data, setup_environment["data_dir"] / "1")


def test_data_task_2(setup_environment):
    static_data = [
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.605555,
                "y": 49.192222,
            },
            "attributes": {
                "id": "1001",
                "vtype": 2,
                "ltype": 1,
                "bearing": 90.0,
                "lineid": 91,
                "linename": "Trolley_91",
                "routeid": 101,
                "course": "12345",
                "lf": "true",
                "delay": 2.0,
                "laststopid": 2001,
                "finalstopid": 2001,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{A1B2C3D4-E5F6-7890-1234-56789ABCDEF0}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.620000,
                "y": 49.200000,
            },
            "attributes": {
                "id": "1002",
                "vtype": 2,
                "ltype": 1,
                "bearing": 180.0,
                "lineid": 92,
                "linename": "Trolley_92",
                "routeid": 102,
                "course": "54321",
                "lf": "true",
                "delay": 0.0,
                "laststopid": 2002,
                "finalstopid": 2002,
                "isinactive": "false",
                "lastupdate": 1734046845100,
                "globalid": "{F0E1D2C3-B4A5-6789-1234-ABCDEF123456}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.625000,
                "y": 49.195000,
            },
            "attributes": {
                "id": "1003",
                "vtype": 2,
                "ltype": 1,
                "bearing": 270.0,
                "lineid": 93,
                "linename": "Trolley_93",
                "routeid": 103,
                "course": "67890",
                "lf": "true",
                "delay": 5.0,
                "laststopid": 2003,
                "finalstopid": 2003,
                "isinactive": "false",
                "lastupdate": 1734046845200,
                "globalid": "{1A2B3C4D-5E6F-7890-1234-DEF012345678}",
            },
        },
    ]
    create_data(static_data, setup_environment['data_dir'] / "2")


def test_data_task_3(setup_environment):
    # Define static data for vehicles with delays
    static_data = [
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.602111,
                "y": 49.194444,
            },
            "attributes": {
                "id": "2998",
                "vtype": 3,
                "ltype": 2,
                "bearing": 120.0,
                "lineid": 101,
                "linename": "Line_101",
                "routeid": 201,
                "course": "11223",
                "lf": "true",
                "delay": 0,  # Initial delay
                "laststopid": 3011,
                "finalstopid": 3021,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{AA1BB2CC3-DD4EE5-FF67890-11234567890A}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.602111,
                "y": 49.194444,
            },
            "attributes": {
                "id": "2999",
                "vtype": 3,
                "ltype": 2,
                "bearing": 120.0,
                "lineid": 101,
                "linename": "Line_101",
                "routeid": 201,
                "course": "11223",
                "lf": "true",
                "delay": 0,  # Initial delay
                "laststopid": 3011,
                "finalstopid": 3021,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{AA1BB2CC3-DD4EE5-FF67890-11234567890A}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.602111,
                "y": 49.194444,
            },
            "attributes": {
                "id": "3001",
                "vtype": 3,
                "ltype": 2,
                "bearing": 120.0,
                "lineid": 101,
                "linename": "Line_101",
                "routeid": 201,
                "course": "11223",
                "lf": "true",
                "delay": 15.0,  # Initial delay
                "laststopid": 3011,
                "finalstopid": 3021,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{AA1BB2CC3-DD4EE5-FF67890-11234567890A}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.602111,
                "y": 49.194444,
            },
            "attributes": {
                "id": "3001",
                "vtype": 3,
                "ltype": 2,
                "bearing": 120.0,
                "lineid": 101,
                "linename": "Line_101",
                "routeid": 201,
                "course": "11223",
                "lf": "true",
                "delay": 10.0,  # Initial delay
                "laststopid": 3011,
                "finalstopid": 3021,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{AA1BB2CC3-DD4EE5-FF67890-11234567890A}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.615555,
                "y": 49.199999,
            },
            "attributes": {
                "id": "3002",
                "vtype": 3,
                "ltype": 2,
                "bearing": 210.0,
                "lineid": 102,
                "linename": "Line_102",
                "routeid": 202,
                "course": "44556",
                "lf": "false",
                "delay": 8.0,  # Initial delay
                "laststopid": 3012,
                "finalstopid": 3022,
                "isinactive": "false",
                "lastupdate": 1734046845100,
                "globalid": "{BB2CC3DD4-EE5FF6-7890112-34567890AABB}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.615555,
                "y": 49.199999,
            },
            "attributes": {
                "id": "3002",
                "vtype": 3,
                "ltype": 2,
                "bearing": 210.0,
                "lineid": 102,
                "linename": "Line_102",
                "routeid": 202,
                "course": "44556",
                "lf": "false",
                "delay": 10.0,  # Initial delay
                "laststopid": 3012,
                "finalstopid": 3022,
                "isinactive": "false",
                "lastupdate": 1734046845100,
                "globalid": "{BB2CC3DD4-EE5FF6-7890112-34567890AABB}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.620000,
                "y": 49.195000,
            },
            "attributes": {
                "id": "3003",
                "vtype": 3,
                "ltype": 2,
                "bearing": 300.0,
                "lineid": 103,
                "linename": "Line_103",
                "routeid": 203,
                "course": "77889",
                "lf": "true",
                "delay": 20.0,  # Initial delay
                "laststopid": 3013,
                "finalstopid": 3023,
                "isinactive": "false",
                "lastupdate": 1734046845200,
                "globalid": "{CC3DD4EE5-FF6789-0112345-67890AABBCD3}",
            },
        },
        {
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.620000,
                "y": 49.195000,
            },
            "attributes": {
                "id": "3003",
                "vtype": 3,
                "ltype": 2,
                "bearing": 300.0,
                "lineid": 103,
                "linename": "Line_103",
                "routeid": 203,
                "course": "77889",
                "lf": "true",
                "delay": 10.0,  # Initial delay
                "laststopid": 3013,
                "finalstopid": 3023,
                "isinactive": "false",
                "lastupdate": 1734046845200,
                "globalid": "{CC3DD4EE5-FF6789-0112345-67890AABBCD3}",
            },
        },
    ]
    create_data(static_data, setup_environment['data_dir'] / "3")


if __name__ == "__main__":
    args = ArgumentParser()
    args.add_argument("--task", type=int, help="Task number to generate test data for.")
    args = vars(args.parse_args())
    if args["task"] == 1:
        test_data_task_1(setup_environment())
    elif args["task"] == 2:
        test_data_task_2(setup_environment())
    elif args["task"] == 3:
        test_data_task_3(setup_environment())
    else:
        print("Please provide a valid task number.")
        exit(1)
