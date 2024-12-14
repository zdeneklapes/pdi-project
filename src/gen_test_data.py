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
        answer = input(f"Directory {output_dir} already exists. Do you want to delete it? Generated will be {len(test_data)} files (records). [y/n]: ")
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
        _lastupdate = datetime.datetime.fromtimestamp(record['attributes']['lastupdate'] / 1000).strftime("%Y-%m-%d %H:%M:%S")
        # _lastupdate = record['attributes']['lastupdate']
        print(f"Writing test data to {file_path} - with lastupdate: {_lastupdate}")
        with open(file_path, "w") as f:
            json.dump(record, f)


def test_data_task_1(setup_environment):
    """Create deterministic mock data for testing all tasks."""
    # Set a fixed random seed for reproducibility
    random.seed(42)
    test_data = [
        *[{
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.605555,
                "y": 49.192222,
            },
            "attributes": {
                "id": str(1000),
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
                "lastupdate": 1734000000000 + (i * 5 * 1000),
                "globalid": "{A1B2C3D4-E5F6-7890-1234-56789ABCDEF0}",
            },
        } for i in range(5)],
        *[{
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.620000,
                "y": 49.200000,
            },
            "attributes": {
                "id": str(1001),
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
                "lastupdate": 1734000000000 + (i * 5 * 1000),
                "globalid": "{F0E1D2C3-B4A5-6789-1234-ABCDEF123456}",
            },
        } for i in range(5)],
    ]

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
                "laststopid": 999,
                "finalstopid": 1000,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{A1B2C3D4-E5F6-7890-1234-56789ABCDEF0}",
            },
        },
        *[{
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
                "laststopid": 2000 + i,
                "finalstopid": 2000 + i,
                "isinactive": "false",
                "lastupdate": 1734046845000,
                "globalid": "{A1B2C3D4-E5F6-7890-1234-56789ABCDEF0}",
            },
        } for i in range(3)],
    ]
    create_data(static_data, setup_environment['data_dir'] / "2")


def test_data_task_3(setup_environment):
    count_data = 4
    # Define static data for vehicles with delays
    static_data = [
        *[{
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.602111,
                "y": 49.194444,
            },
            "attributes": {
                "id": str(2000 + i),
                "vtype": 3,
                "ltype": 2,
                "bearing": 120.0,
                "lineid": 101,
                "linename": "Line_101",
                "routeid": 201,
                "course": "11223",
                "lf": "true",
                "delay": 0,
                "laststopid": 3011,
                "finalstopid": 3021,
                "isinactive": "false",
                "lastupdate": 1734040000100,
                "globalid": "{AA1BB2CC3-DD4EE5-FF67890-11234567890A}",
            },
        } for i in range(2)],
        *[
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
                    "delay": 20.0 - (i),
                    "laststopid": 3011,
                    "finalstopid": 3021,
                    "isinactive": "false",
                    "lastupdate": 1734040000000 + (i * 5 * 1000),
                    "globalid": "{AA1BB2CC3-DD4EE5-FF67890-11234567890A}",
                },
            } for i in range(count_data)
        ],
        *[{
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
                "delay": 20.0 - (2 * i),
                "laststopid": 3012,
                "finalstopid": 3022,
                "isinactive": "false",
                "lastupdate": 1734040000000 + (i * 5 * 1000),
                "globalid": "{BB2CC3DD4-EE5FF6-7890112-34567890AABB}",
            },
        } for i in range(count_data)],
        *[{
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
                "delay": 20.0 - (3 * i),
                "laststopid": 3013,
                "finalstopid": 3023,
                "isinactive": "false",
                "lastupdate": 1734040000000 + (i * 5 * 1000),
                "globalid": "{CC3DD4EE5-FF6789-0112345-67890AABBCD3}",
            },
        } for i in range(count_data)]
    ]
    create_data(static_data, setup_environment['data_dir'] / "3")


def test_data_task_4(setup_environment):
    static_data = [
        *[{
            "geometry": {
                "spatialReference": {"wkid": 4326},
                "x": 16.640000,
                "y": 49.220000,
            },
            "attributes": {
                "id": str(1000+i),
                "vtype": 3,
                "ltype": 2,
                "bearing": 45.0,
                "lineid": 95,
                "linename": "Line_95",
                "routeid": 105,
                "course": "11234",
                "lf": "false",
                "delay": 10.0,
                "laststopid": 2009,
                "finalstopid": 2010,
                "isinactive": "false",
                "lastupdate": 1734040000000 + (i * 60 * 1000),
                "globalid": "{3C4D5E6F-7G8H-9012-3456-7890ABCDEFAB}",
            },
        } for i in range(9)],
    ]
    create_data(static_data, setup_environment['data_dir'] / "4")

def test_data_task_5(setup_environment):
    """
    Generate deterministic mock data for testing Task 5.
    Task 5 involves calculating the min/max interval of the last 10 updates.
    """
    COUNT_DATA = 10
    static_data = [
        *[
            {
                "geometry": {
                    "spatialReference": {"wkid": 4326},
                    "x": 16.605555 + (i * 0.0001),  # Simulating slight movement
                    "y": 49.192222 + (i * 0.0001),
                },
                "attributes": {
                    "id": "5001",
                    "vtype": 3,
                    "ltype": 2,
                    "bearing": 90.0,
                    "lineid": 101,
                    "linename": "Line_101",
                    "routeid": 201,
                    "course": "98765",
                    "lf": "true",
                    "delay": random.uniform(0, 50),  # Random delay to simulate real-world variability
                    "laststopid": 4000 + i,
                    "finalstopid": 5000 + i,
                    "isinactive": "false",
                    "lastupdate": 1734040000000 + (i * 10 * 1000),  # 1-minute interval
                    "globalid": f"{{AA1BB2CC3-DD4EE5-FF67890-11234567890A-{i}}}",
                },
            }
            for i in range(COUNT_DATA)
        ],
        *[
            {
                "geometry": {
                    "spatialReference": {"wkid": 4326},
                    "x": 16.615555 + (i * 0.0002),  # Simulating slight movement
                    "y": 49.199999 + (i * 0.0002),
                },
                "attributes": {
                    "id": "5002",
                    "vtype": 3,
                    "ltype": 2,
                    "bearing": 180.0,
                    "lineid": 102,
                    "linename": "Line_102",
                    "routeid": 202,
                    "course": "44556",
                    "lf": "true",
                    "delay": random.uniform(0, 50),
                    "laststopid": 4001 + i,
                    "finalstopid": 5001 + i,
                    "isinactive": "false",
                    "lastupdate": 1734040000000 + (i * 10 * 1000),
                    "globalid": f"{{BB2CC3DD4-EE5FF6-7890112-34567890AABB-{i}}}",
                },
            }
            for i in range(COUNT_DATA)
        ],
        *[
            {
                "geometry": {
                    "spatialReference": {"wkid": 4326},
                    "x": 16.625555 + (i * 0.0003),  # Simulating slight movement
                    "y": 49.205555 + (i * 0.0003),
                },
                "attributes": {
                    "id": "5003",
                    "vtype": 3,
                    "ltype": 2,
                    "bearing": 270.0,
                    "lineid": 103,
                    "linename": "Line_103",
                    "routeid": 203,
                    "course": "77889",
                    "lf": "true",
                    "delay": random.uniform(0, 50),
                    "laststopid": 4002 + i,
                    "finalstopid": 5002 + i,
                    "isinactive": "false",
                    "lastupdate": 1734040000000 + (i * 10 * 1000),
                    "globalid": f"{{CC3DD4EE5-FF6789-0112345-67890AABBCD3-{i}}}",
                },
            }
            for i in range(COUNT_DATA)
        ],
    ]
    create_data(static_data, setup_environment['data_dir'] / "5")


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
    elif args["task"] == 4:
        test_data_task_4(setup_environment())
    elif args["task"] == 5:
        test_data_task_5(setup_environment())
    else:
        print("Please provide a valid task number.")
        exit(1)
