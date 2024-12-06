import asyncio
import json
import os
from argparse import ArgumentParser
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from pprint import pformat
from typing import Tuple

import aiofile
from pyflink.common import Row
from pyflink.common import Duration, Encoder
from pyflink.common import Types
from pyflink.common import WatermarkStrategy
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    FileSystemCheckpointStorage,
    DataStream,
)
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.functions import KeySelector
from websockets import connect

ENVIRONMENT = {"PRINT": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]}
DEBUG = True if "DEBUG" in ENVIRONMENT["PRINT"] else False

ROOT_DIR = Path(__file__).parent.parent
SRC_DIR = ROOT_DIR / "src"


class BoundingBox(Enum):
    LAT_MIN = 0
    LAT_MAX = 1
    LNG_MIN = 2
    LNG_MAX = 3


@dataclass()
class Program:
    args: dict = field(default_factory=dict)


class Logger:
    instance = None

    def __new__(cls):
        if cls.instance is None:
            cls.instance = super(Logger, cls).__new__(cls)
            cls.instance.log = []
        return cls.instance

    def _get_time(self):
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def debug(self, message):
        if "DEBUG" in ENVIRONMENT["PRINT"]:
            print(f"DEBUG ({self._get_time()}): {message}")

    def info(self, message):
        if "INFO" in ENVIRONMENT["PRINT"]:
            print(f"INFO ({self._get_time()}): {message}")

    def warning(self, message):
        if "WARNING" in ENVIRONMENT["PRINT"]:
            print(f"WARNING: {message}")

    def _assert(self, condition, message):
        if not condition:
            assert False, message

    def overwrite(self, message):
        print(f"\r{message}", end="")


logger = Logger()


# class ReduceDelayProcessFunction(KeyedProcessFunction):
#     def process_element(self, vehicle, ctx: "KeyedProcessFunction.Context", out):
#         prev_delay = self.get_runtime_context().get_state(ValueStateDescriptor("prev_delay", Types.DOUBLE()))
#         curr_delay = vehicle[PublicTransitKey.delay]
#         if prev_delay.value() is not None and curr_delay < prev_delay.value():
#             delay_diff = prev_delay.value() - curr_delay
#             out.collect((vehicle[PublicTransitKey.id], delay_diff))
#         prev_delay.update(curr_delay)
#
#
# class ComputeIntervalsFunction(FlatMapFunction):
#     def __init__(self):
#         self.timestamps = []
#
#     def flat_map(self, vehicle, out):
#         self.timestamps.append(vehicle[PublicTransitKey.lastupdate])
#         if len(self.timestamps) > 10:
#             self.timestamps.pop(0)
#         if len(self.timestamps) > 1:
#             intervals = [self.timestamps[i] - self.timestamps[i - 1] for i in range(1, len(self.timestamps))]
#             out.collect((min(intervals), max(intervals)))
#
#
# class CustomTimestampAssigner(TimestampAssigner):
#     def extract_timestamp(self, value, record_timestamp):
#         return value["attributes"]["lastupdate"]
#
#
# class MinMaxDelayAggregate(AggregateFunction):
#     """
#     Aggregate function to calculate the minimum and maximum delays.
#     """
#
#     def create_accumulator(self):
#         """
#         Initializes the accumulator with default min and max values.
#         """
#         return float("inf"), float("-inf")  # (min_delay, max_delay)
#
#     def add(self, value, accumulator):
#         """
#         Updates the accumulator with a new delay value.
#         :param value: The current vehicle data (expects delay field).
#         :param accumulator: Tuple (min_delay, max_delay).
#         """
#         delay = value[PublicTransitKey.delay]
#         min_delay, max_delay = accumulator
#         return min(delay, min_delay), max(delay, max_delay)
#
#     def get_result(self, accumulator):
#         """
#         Returns the min and max delays from the accumulator.
#         """
#         return accumulator
#
#     def merge(self, acc1, acc2):
#         """
#         Merges two accumulators.
#         """
#         return min(acc1[0], acc2[0]), max(acc1[1], acc2[1])
#


async def get_data_source(program: Program, settings: dict) -> Tuple[StreamExecutionEnvironment, DataStream]:
    """
    Get the Flink environment.
    :param message_store:
    :param program:
    :param settings:
    :return:
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # start a checkpoint every 1000 ms
    env.enable_checkpointing(1000)
    # set mode to exactly-once (this is the default)
    env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    # make sure 500 ms of progress happen between checkpoints
    env.get_checkpoint_config().set_min_pause_between_checkpoints(500)
    # checkpoints have to complete within one minute, or are discarded
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    # only two consecutive checkpoint failures are tolerated
    env.get_checkpoint_config().set_tolerable_checkpoint_failure_number(2)
    # allow only one checkpoint to be in progress at the same time
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    # enable externalized checkpoints which are retained after job cancellation
    # env.get_checkpoint_config().set_externalized_checkpoint_retention(
    #     ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    # )
    # enables the unaligned checkpoints
    env.get_checkpoint_config().enable_unaligned_checkpoints()

    # set the checkpoint storage to file system
    checkpoint_dir = ROOT_DIR / "tmp/checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    file_storage = FileSystemCheckpointStorage(f"file://{checkpoint_dir}")
    env.get_checkpoint_config().set_checkpoint_storage(file_storage)
    # settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    # table_env = StreamTableEnvironment.create(stream_execution_environment=stream_env, environment_settings=settings)

    # mode: BULK, BATCH
    logger.debug(f"Processing mode: {program.args['mode']}, directory: {program.args['data_dir']}")
    if program.args["mode"] == "batch":
        data_source = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(), program.args["data_dir"].as_posix())
            .process_static_file_set()
            .build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="FileSource",
        )
    elif program.args["mode"] == "stream":
        data_source = env.from_source(
            source=FileSource.for_record_stream_format(StreamFormat.text_line_format(), program.args["data_dir"].as_posix())
            .monitor_continuously(Duration.of_seconds(4))
            .build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="FileSource",
        )
    else:
        assert False, f"Invalid mode: {program.args['mode']}"

    return env, data_source


async def preprocess_data(data_source: DataStream, program: Program) -> DataStream:
    """
    Preprocess the data before executing the tasks.
    """
    logger.debug("Preprocessing the data.")

    def json_to_dict(json_record):
        """
        Convert a JSON record to a dictionary.
        """
        try:
            return json.loads(json_record)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON record: {json_record}. Error: {e}")
            return None

    def dict_to_row(record):
        """
        Convert dictionary to Flink Row object for proper processing.
        """
        return Row(record.get("geometry"), record.get("attributes"))


    # Convert JSON records to Python dictionaries
    data_source = data_source.map(
        json_to_dict, output_type=Types.MAP(Types.STRING(), Types.STRING())
    ).filter(lambda record: record is not None)

    # data_source = data_source.map(
    #     dict_to_row,
    #     output_type=Types.ROW_NAMED(
    #         ["geometry", "attributes"],
    #         [Types.MAP(Types.STRING(), Types.FLOAT()), Types.MAP(Types.STRING(), Types.STRING())]
    #     )
    # )

    # Filter out inactive vehicles (isinactive = "false")
    data_source = data_source.filter(
        lambda record: record.get("attributes", {}).get("isinactive", None) == "false"
    )

    # Step 3: Key the stream by `id` (or another unique attribute) for further processing
    class KeyById(KeySelector):
        def get_key(self, value):
            return str(value.get("attributes", {}).get("id", None))

    data_source = data_source.key_by(KeyById())

    # Define a sink to save the preprocessed data (if required)
    sink_dir = program.args["output_dir"] / "preprocessed_data"
    sink_dir.mkdir(parents=True, exist_ok=True)

    sink = FileSink.for_row_format(
        base_path=str(sink_dir),
        encoder=Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("preprocessed")
        .with_part_suffix(".json")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ).build()

    # Sink preprocessed data
    data_source.sink_to(sink)

    logger.debug("Preprocessing completed and data has been written to the sink.")

    return data_source


async def task_1(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 1: Print vehicles in a specified area.
    """
    logger.debug("Task 1: Filtering vehicles in the specified bounding box.")
    bounding_box = program.args["bounding_box"]

    def filter_out_inactive_vehicles(vehicle):
        """
        Helper function to filter out inactive vehicles.
        """
        formatted = pformat(vehicle)
        logger.debug(f"Vehicle in bounding box: \n{formatted}")
        isinactive = vehicle["attributes"]["isinactive"]
        assert isinactive is not None, f"Invalid vehicle attributes: {isinactive}"
        if isinstance(isinactive, str):
            isinactive = isinactive.lower()
            assert isinactive in ["true", "false"], f"Invalid vehicle attributes: {isinactive}"
        if isinactive == "true":
            isinactive = True
        elif isinactive == "false":
            isinactive = False
        else:
            assert False, f"Invalid vehicle attributes: {isinactive}"
        return isinactive == False

    def filter_out_of_bounding_box(vehicle: dict):
        """
        Helper function to filter and log vehicles.
        """
        _x = vehicle.get("geometry", {}).get("x", None)
        _y = vehicle.get("geometry", {}).get("y", None)
        assert _x is not None and _y is not None, f"Invalid vehicle geometry: {_x}, {_y}"
        in_bounding_box = (
                bounding_box[BoundingBox.LAT_MIN] <= _y <= bounding_box[BoundingBox.LAT_MAX]
                and bounding_box[BoundingBox.LNG_MIN] <= _x <= bounding_box[BoundingBox.LNG_MAX]
        )
        logger.debug(f"Vehicle in bounding box: {in_bounding_box}")
        return in_bounding_box

    filtered_data = (data_source
                     .filter(lambda vehicle: filter_out_inactive_vehicles(vehicle))
                     .filter(lambda vehicle: filter_out_of_bounding_box(vehicle))
                     )
    filtered_data.print()
    return filtered_data


# async def task_2(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 2: List trolleybuses at their final stop.
#     """
#     logger.debug("Task 2: Filtering trolleybuses at their final stop.")
#     trolleybuses_at_final_stop = data_source.filter(
#         lambda vehicle: vehicle[PublicTransitKey.ltype] == 2  # Assuming ltype 2 indicates trolleybus
#                         and vehicle[PublicTransitKey.laststopid] == vehicle[PublicTransitKey.finalstopid]
#     ).map(
#         lambda vehicle: {
#             "id": vehicle[PublicTransitKey.id],
#             "stop": vehicle[PublicTransitKey.laststopid],
#             "timestamp": vehicle[PublicTransitKey.lastupdate],
#         }
#     )
#     trolleybuses_at_final_stop.print()
#     return trolleybuses_at_final_stop
#
#
# async def task_3(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 3: List delayed vehicles reducing delay, sorted by improvement.
#     """
#     logger.debug("Task 3: Finding delayed vehicles reducing delay.")
#     vehicles_with_delay = data_source.key_by(lambda vehicle: vehicle[PublicTransitKey.id]).process(
#         ReduceDelayProcessFunction()
#     )
#     vehicles_with_delay.print()
#     return vehicles_with_delay
#
#
# async def task_4(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 4: Min/max delay in the last 3 minutes.
#     """
#     logger.debug("Task 4: Calculating min/max delay in the last 3 minutes.")
#     watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10)).with_timestamp_assigner(
#         CustomTimestampAssigner()
#     )
#     delay_stream = data_source.assign_timestamps_and_watermarks(watermark_strategy).key_by(
#         lambda vehicle: vehicle[PublicTransitKey.id]
#     ).window(
#         SlidingProcessingTimeWindows.of(Time.minutes(3), Time.seconds(10))
#     ).aggregate(
#         MinMaxDelayAggregate(),
#         output_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()])
#     )
#     delay_stream.print()
#     return delay_stream
#
#
# async def task_5(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 5: Min/max interval of the last 10 updates.
#     """
#     logger.debug("Task 5: Calculating min/max intervals of the last 10 updates.")
#
#     def map_vehicle_to_key(vehicle):
#         return vehicle.get('attributes', {}).get('vehicleid', None)
#
#     interval_stream = data_source.key_by(
#         lambda vehicle: map_vehicle_to_key(vehicle)
#     ).flat_map(ComputeIntervalsFunction())
#     interval_stream.print()
#     return interval_stream


async def process_tasks(program: Program):
    """
    Process the specified task based on the program arguments.
    """
    TASKS = {
        1: task_1,
        # 2: task_2,
        # 3: task_3,
        # 4: task_4,
        # 5: task_5,
    }
    logger.debug(f"Starting task processing: Task {program.args['task']}.")

    # Get the data source
    env, data_source = await get_data_source(program, settings={})

    # Preprocess the data
    data_source = await preprocess_data(data_source, program)

    # if 0 in program.args["task"]:
    #     # Execute all tasks if task 0 is specified
    #     logger.debug("Task 0: Executing all tasks.")
    #     for task_number, task_function in TASKS.items():
    #         logger.debug(f"Executing Task {task_number}.")
    #         data_source = await task_function(data_source, program)
    # else:
    #     # Execute the specified task
    #     for task_number in program.args["task"]:
    #         logger.debug(f"Executing Task {program.args['task']}.")
    #         data_source = await TASKS[task_number](data_source, program)

    # Execute the Flink job
    logger.debug("Task processing started.")
    env.execute("Public Transit Stream Processing")
    logger.debug("Task processing completed.")


class WebStreamStore:
    URI = "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326"

    def __init__(self, program):
        # self.messages = asyncio.Queue()
        self.program = program
        self.websocket = None

    async def __aenter__(self):
        self.connection = connect(self.URI)
        self.websocket = await self.connection.__aenter__()
        logger.info(f"Connected to websocket: {self.URI}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.connection.__aexit__(exc_type, exc_val, exc_tb)

    def _get_file_name(self):
        timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return self.program.args["data_dir"] / f"{timestamp}.json"

    async def receive(self):
        """Receive messages continuously and store them."""
        saved_files = 0
        while True:
            message = await self.websocket.recv()
            await self.save_to_file(message)
            saved_files += 1
            logger.overwrite(f"Saved {saved_files} files")

    async def save_to_file(self, message):
        file_name = self._get_file_name()
        async with aiofile.async_open(file_name, "w") as f:
            await f.write(message)

    # async def receive_and_save(self):
    #     while True:
    #         message = await self.websocket.recv()
    #         await self.save_to_file(message)

    # def is_file_processed(self):
    #     try:
    #         return self.file_processed
    #     except AttributeError:
    #         self.file_processed = False
    #         return self.file_processed

    # async def get_messages(self) -> list:
    #     """ Retrieve all messages and clear the store. """
    #     _all = []
    #     if self.program.args["process_data_file"]:
    #         if self.is_file_processed():
    #             raise Exception("File already processed.")
    #         self.file_processed = True
    #         # Open the file and read JSON records
    #         async with aiofile.async_open(self.program.args["process_data_file"], "r") as file:
    #             record = None
    #             async for line in file:
    #                 try:
    #                     record = record + line if record else line
    #                 except json.JSONDecodeError as e:
    #                     logger.warning(f"Failed to parse JSON record: {line}. Error: {e}")
    #         return _all
    #     else:
    #         while not self.messages.empty():
    #             _all.append(json.loads(await self.messages.get()))
    #         return _all


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
        nargs="+",
        default=[0],
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
    # args.add_argument(
    #     "--checkpoint_dir",
    #     type=str,
    #     default=ROOT_DIR / "tmp/checkpoints",
    #     help="Directory for storing checkpoints.",
    # )

    _args = args.parse_args()

    _args.output_dir = Path(os.path.abspath(_args.output_dir))
    _args.output_dir.mkdir(parents=True, exist_ok=True)

    _args.data_dir = Path(os.path.abspath(_args.data_dir))
    _args.data_dir.mkdir(parents=True, exist_ok=True)

    for task_number in _args.task:
        assert 0 <= task_number <= 5, f"Invalid task number: {task_number}"

    # _args.checkpoint_dir = Path(os.path.abspath(_args.checkpoint_dir))
    # _args.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    return vars(_args)


async def main():
    program = Program(parseArgs())
    logger.debug(f"python3 ./src/main.py --bounding-box " + " ".join(map(str, program.args.get("bounding_box"))))
    logger.debug(f"Program arguments: {program.args}")
    async with WebStreamStore(program) as store:
        if program.args["mode"] == "download":
            await store.receive()
        elif program.args["mode"] == "batch":
            await process_tasks(program)
        elif program.args["mode"] == "stream":
            receiver_task = asyncio.create_task(store.receive())
            processor_task = asyncio.create_task(process_tasks(program))
            await asyncio.wait([receiver_task, processor_task], return_when=asyncio.FIRST_COMPLETED)
        else:
            assert False, f"Invalid mode: {program.args['mode']}"


if __name__ == "__main__":
    logger.debug("Running in debug mode.")
    asyncio.run(main(), debug=DEBUG)
