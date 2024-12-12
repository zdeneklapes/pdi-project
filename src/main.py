import json
import os
import sys
from argparse import ArgumentParser
from datetime import datetime
from multiprocessing import Process
from pathlib import Path

import websockets

from lib.flink import process_tasks
from lib.mylogger import MyLogger
from lib.program import Program


def download_data(program: Program):
    """Run the data downloading process."""
    uri = "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326"
    program.logger.info(f"Starting data download from {uri}")

    def save_to_file(message, program):
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        file_name = program.args["data_dir"] / f"{timestamp}.json"
        with open(file_name, "w") as f:
            f.write(message)

    try:

        async def receive_messages():
            saved_files = 0
            async with websockets.connect(uri) as websocket:
                program.logger.info(f"Connected to websocket: {uri}")
                while True:
                    message = await websocket.recv()
                    # _as_dict = json.loads(message)
                    # from pprint import pprint
                    # pprint(_as_dict)
                    save_to_file(message, program)
                    saved_files += 1
                    program.logger.debug_overwrite(f"Saved {saved_files} files")

        import asyncio

        asyncio.run(receive_messages())

    except Exception as e:
        program.logger.warning(f"Error in WebSocket connection: {e}")
    finally:
        program.logger.info("Data downloading process completed.")


def parseArgs() -> dict:
    args = ArgumentParser()
    args.add_argument(
        "--bounding-box",
        nargs=+4,
        type=float,
        required=True,
        help="Bounding box for filtering vehicles: lat_min | lat_max | lng_min | lng_max",
    )
    args.add_argument(
        "--task",
        type=int,
        # nargs="+",
        default=1,
        required=True,
        help=(
            """
            Task number (1–5):
            0: all tasks
            1: Print vehicles in a specified area.
            2: List trolleybuses at their final stop.
            3: List delayed vehicles reducing delay, sorted by improvement.
            4: Min/max delay in the last 3 minutes.
            5: Min/max interval of the last 10 updates.
        """
        ),
    )
    args.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Max time to wait for process one message.",
    )
    args.add_argument(
        "--output_dir",
        type=str,
        required=True,
        help="Output directory for the results.",
    )
    args.add_argument(
        "--data_dir",
        type=str,
        required=True,
        help="Directory for storing messages.",
    )
    args.add_argument(
        "--mode",
        type=str,
        choices=["batch", "stream", "download"],
        default="stream",
        help=(
            """
        Mode for processing the data:
        batch: Process the data in given directory as a batch (process only already downloaded files).
        stream: Process the data in given directory as a stream (process and download new files simultaneously).
        download: Download the data from the websocket (do not process the data).
        """
        ),
    )
    args.add_argument(
        "--log",
        nargs="+",
        default=[
            'DEBUG',
            'INFO',
            "WARNING",
            "ERROR",
            "CRITICAL",
        ],
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )

    _args = args.parse_args()

    _args.output_dir = Path(os.path.abspath(_args.output_dir))
    _args.output_dir.mkdir(parents=True, exist_ok=True)

    _args.data_dir = Path(os.path.abspath(_args.data_dir))
    _args.data_dir.mkdir(parents=True, exist_ok=True)

    return vars(_args)


# MAIN
if __name__ == "__main__":
    logger = MyLogger(parseArgs()["log"])
    program = Program(parseArgs(), logger)
    program.logger.info(f"Program arguments: {program.args}")
    program.logger.info(f"Run: python3 {' '.join(sys.argv)}")
    try:
        if program.args["mode"] == "download":
            download_process = Process(target=download_data, args=(program,))
            download_process.start()
            download_process.join()
        elif program.args["mode"] == "batch":
            process_tasks(program)
        elif program.args["mode"] == "stream":
            download_process = Process(target=download_data, args=(program,))
            download_process.start()
            process_tasks(program)
            download_process.join()
        else:
            raise ValueError(f"Invalid mode: {program.args['mode']}")
    except Exception as e:
        program.logger.warning(f"Warn in main process: {e}")
        exit(1)
