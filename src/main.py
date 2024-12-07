import asyncio
import json
import os
import sys
from argparse import ArgumentParser
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Tuple

import aiofile
from pyflink.common import Duration, Encoder
from pyflink.common import Row
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
        if "DEBUG" in ENVIRONMENT["PRINT"]:
            print(f"DEBUG: \r{message}", end="")


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

async def get_env() -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    cpus = os.cpu_count() // 2 or 1
    logger.info(f"Setting parallelism to {cpus}")
    env.set_parallelism(cpus)
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
    # TODO: is not supported in JVM 17 ???
    # env.get_checkpoint_config().set_externalized_checkpoint_retention(
    #     ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    # )
    # enables the unaligned checkpoints
    env.get_checkpoint_config().enable_unaligned_checkpoints()

    env.set_buffer_timeout(20000)  # Set buffer timeout to 20 seconds
    # set the checkpoint storage to file system
    checkpoint_dir = ROOT_DIR / "tmp/checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    file_storage = FileSystemCheckpointStorage(f"file://{checkpoint_dir}")
    env.get_checkpoint_config().set_checkpoint_storage(file_storage)
    # settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    # table_env = StreamTableEnvironment.create(stream_execution_environment=stream_env, environment_settings=settings)

    return env


async def get_data_source(program: Program, settings: dict) -> Tuple[StreamExecutionEnvironment, DataStream]:
    """
    Get the Flink environment.
    :param program:
    :param settings:
    :return:
    """
    env = await get_env()
    # mode: BULK, BATCH
    logger.debug(f"Output directory: {program.args['data_dir']}")
    if program.args["mode"] == "batch":
        logger.debug("Processing data in BATCH mode.")
        data_source = env.from_source(
            source=FileSource.for_record_stream_format(
                StreamFormat.text_line_format(),
                program.args["data_dir"].as_posix()
            )
            .process_static_file_set()
            # .monitor_continuously(Duration.of_seconds(10)) # TODO: Remove
            .build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
            source_name="FileSource",
        )
    elif program.args["mode"] == "stream":
        logger.debug("Processing data in STREAM mode.")
        data_source = env.from_source(
            source=FileSource.for_record_stream_format(
                StreamFormat.text_line_format(),
                program.args["data_dir"].as_posix()
            )
            .monitor_continuously(Duration.of_seconds(10))
            .build(),
            watermark_strategy=WatermarkStrategy.for_monotonous_timestamps().with_idleness(Duration.of_seconds(30)),
            source_name="FileSource",
        )
    else:
        env.close()
        assert False, f"Invalid mode: {program.args['mode']}"

    return env, data_source


def format_preprocessed_data(record: Row):
    """
    Print the formatted data.
    """
    row = record.as_dict()
    str_stdout = (
        f"id: {row['id']:>6} | "
        f"vtype: {row['vtype']:>2} | "
        f"ltype: {row['ltype']:>2} | "
        f"lat: {row['lat']:>2.4f} | "
        f"lng: {row['lng']:>2.4f} | "
        f"bearing: {row['bearing']:>5.1f} | "
        f"lineid: {row['lineid']:>4} | "
        # f"linename: {row['linename']:>2} | "
        f"routeid: {row['routeid']:>5} | "
        # f"course: {row['course']:>2} | "
        # f"lf: {row['lf']:>2} | "
        # f"delay: {row['delay']:>4.1f} | "
        # f"laststopid: {row['laststopid']:>5} | "
        # f"finalstopid: {row['finalstopid']:>5} | "
        # f"isinactive: {row['isinactive']:>5} | "
        f"lastupdate: {row['lastupdate']:>15} | "
        # f"globalid: {row['globalid']:>5}"
    )
    return str_stdout


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
        Convert dictionary to a flat Flink Row object for proper processing.
        Includes flattening geometry and attributes into top-level fields.
        """
        geometry = record.get("geometry", {})
        attributes = record.get("attributes", {})

        return Row(
            id=attributes.get("id"),
            vtype=attributes.get("vtype"),
            ltype=attributes.get("ltype"),
            lat=geometry.get("y"),
            lng=geometry.get("x"),
            bearing=attributes.get("bearing"),
            lineid=attributes.get("lineid"),
            linename=attributes.get("linename"),
            routeid=attributes.get("routeid"),
            course=attributes.get("course"),
            lf=attributes.get("lf"),
            delay=attributes.get("delay"),
            laststopid=attributes.get("laststopid"),
            finalstopid=attributes.get("finalstopid"),
            isinactive=attributes.get("isinactive"),
            lastupdate=attributes.get("lastupdate"),
            globalid=attributes.get("globalid"),
        )

    # Convert JSON records to Python dictionaries
    data_source = data_source.map(
        json_to_dict, output_type=Types.MAP(Types.STRING(), Types.STRING())
    ).filter(lambda record: record is not None)

    # Flatten and structure records into Rows
    data_source = data_source.map(
        dict_to_row,
        output_type=Types.ROW_NAMED(
            [
                "id", "vtype", "ltype", "lat", "lng", "bearing", "lineid", "linename",
                "routeid", "course", "lf", "delay", "laststopid", "finalstopid",
                "isinactive", "lastupdate", "globalid"
            ],
            [
                Types.STRING(), Types.INT(), Types.INT(), Types.FLOAT(), Types.FLOAT(),
                Types.FLOAT(), Types.INT(), Types.STRING(), Types.INT(), Types.STRING(),
                Types.STRING(), Types.FLOAT(), Types.INT(), Types.INT(),
                Types.STRING(), Types.LONG(), Types.STRING()
            ]
        )
    )

    # Filter out inactive vehicles (isinactive = "false")
    data_source = data_source.filter(
        lambda record: record.isinactive == "false"
    )

    # Step 3: Key the stream by `id` (or another unique attribute) for further processing
    class KeyById(KeySelector):
        def get_key(self, value):
            return value.id

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
        .with_part_suffix(".txt")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ).build()

    # Sink preprocessed data
    data_source.sink_to(sink)

    # formatted_data = data_source.map(
    #     print_all_data_formatted,
    #     output_type=Types.STRING()
    # )
    # formatted_data.print()

    logger.debug("Preprocessing completed and data has been written to the sink.")

    return data_source


async def task_1(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 1: Print vehicles in the specified bounding box and save the results.
    """
    logger.debug("Task 1: Filtering vehicles in the specified bounding box.")
    bounding_box = program.args["bounding_box"]

    def is_within_bounding_box(vehicle: Row):
        """
        Check if a vehicle is within the bounding box.
        """
        row = vehicle.as_dict()
        lat = row.get("lat", None)
        lon = row.get("lng", None)
        assert lat is not None and lon is not None, f"Invalid vehicle geometry: {vehicle}"
        in_box = (
                bounding_box[BoundingBox.LAT_MIN.value] <= lat <= bounding_box[BoundingBox.LAT_MAX.value]
                and bounding_box[BoundingBox.LNG_MIN.value] <= lon <= bounding_box[BoundingBox.LNG_MAX.value]
        )
        return in_box

    # Filter vehicles within the bounding box
    filtered_data = data_source.filter(is_within_bounding_box)

    # Key vehicles by ID for possible downstream operations
    class KeyById(KeySelector):
        def get_key(self, value: Row):
            row = value.as_dict()
            return row.get("id")

    keyed_data = filtered_data.key_by(KeyById())

    # PRINT
    asyncio.sleep(2)
    formatted_data = data_source.map(
        format_preprocessed_data,
        output_type=Types.STRING()
    )
    formatted_data.print()
    asyncio.sleep(2)

    # SINK
    sink_dir = program.args["output_dir"] / "task1"
    sink_dir.mkdir(parents=True, exist_ok=True)
    sink = FileSink.for_row_format(
        base_path=str(sink_dir),
        encoder=Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("task1")
        .with_part_suffix(".txt")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ).build()
    data_source.sink_to(sink)

    logger.debug("Task 1 completed. Results printed and saved.")
    return keyed_data


async def task_2(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 2: List trolleybuses at their final stop, with stop ID and time of arrival.
    """
    logger.debug("Task 2: Filtering trolleybuses that have reached their final stop.")

    def is_trolleybus_at_final_stop(vehicle: Row):
        """
        Check if a trolleybus has reached its final stop.
        """
        row = vehicle.as_dict()
        vtype = row.get("vtype", None)  # Assuming vtype indicates the vehicle type
        last_stop = row.get("laststopid", None)
        final_stop = row.get("finalstopid", None)

        # Ensure the required fields are present
        assert vtype is not None and last_stop is not None and final_stop is not None, \
            f"Invalid vehicle data: {row}"

        # Check if the vehicle is a trolleybus and has reached its final stop
        return vtype == 2 and last_stop == final_stop  # Assuming vtype 2 indicates a trolleybus

    def format_trolleybus_data(vehicle: Row):
        """
        Format trolleybus data for logging or saving.
        """
        row = vehicle.as_dict()
        return (
            f"id: {row.get('id'):>6} | "
            f"laststopid: {row.get('laststopid'):>5} | "
            f"finalstopid: {row.get('finalstopid'):>5} | "
        )

    # Filter trolleybuses that have reached their final stop
    filtered_data = data_source.filter(is_trolleybus_at_final_stop)

    # PRINT
    filtered_data.map(
        format_trolleybus_data,
        output_type=Types.STRING()
    ).print()

    # with filtered_data.execute_and_collect() as results:
    #     for result in results:
    #         print(result)

    # SINK
    sink_dir = program.args["output_dir"] / "task2"
    sink_dir.mkdir(parents=True, exist_ok=True)
    sink = FileSink.for_row_format(
        base_path=str(sink_dir),
        encoder=Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("task2")
        .with_part_suffix(".txt")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ).build()
    filtered_data.sink_to(sink)
    # data_source.sink_to(sink)

    logger.debug("Task 2 completed. Results printed and saved.")
    return filtered_data
    # return data_source


# async def task_3(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 3: List delayed vehicles reducing delay, sorted by improvement.
#     """
#     logger.debug("Task 3: Finding delayed vehicles reducing delay.")
#     return foo


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

from concurrent.futures import ThreadPoolExecutor

_executor = ThreadPoolExecutor(os.cpu_count() // 2 or 1)


async def process_tasks(program: Program):
    """
    Process the specified task based on the program arguments.
    """
    TASKS = {
        1: task_1,
        2: task_2,
        # 3: task_3,
        # 4: task_4,
        # 5: task_5,
    }
    logger.debug(f"Starting task processing: Task {program.args['task']}.")

    # Get the data source
    env, data_source = await get_data_source(program, settings={})

    # Preprocess the data
    data_source = await preprocess_data(data_source, program)

    if 0 in program.args["task"]:
        # Execute all tasks if task 0 is specified
        logger.debug("Task 0: Executing all tasks.")
        for task_number, task_function in TASKS.items():
            logger.debug(f"Executing Task {task_number}.")
            data_source = await task_function(data_source, program)
    else:
        # Execute the specified task
        for task_number in program.args["task"]:
            logger.debug(f"Executing Task {program.args['task']}.")
            data_source = await TASKS[task_number](data_source, program)

    # Execute the Flink job
    logger.debug("Task processing started.")
    # run executor in asyncio
    await asyncio.get_event_loop().run_in_executor(_executor, env.execute, "Public Transit Stream Processing")
    # env.execute("Public Transit Stream Processing")
    env.close()
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
        logger.debug("Receiving messages from the websocket.")
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
    logger.debug(f"python3 ./{sys.argv[0]} {' '.join(sys.argv[1:])}")
    logger.debug(f"Program arguments: {program.args}")
    async with WebStreamStore(program) as store:
        if program.args["mode"] == "download":
            logger.info("Downloading data from the websocket.")
            await store.receive()
        elif program.args["mode"] == "batch":
            logger.info("Processing data in BATCH mode.")
            await process_tasks(program)
        elif program.args["mode"] == "stream":
            logger.info("Processing data in STREAM mode.")
            # receiver_task = asyncio.create_task(store.receive())
            # processor_task = asyncio.create_task(process_tasks(program))
            # await asyncio.wait([receiver_task, processor_task], return_when=asyncio.FIRST_COMPLETED)
            await asyncio.gather(store.receive(), process_tasks(program))
        else:
            assert False, f"Invalid mode: {program.args['mode']}"


if __name__ == "__main__":
    logger.debug("Running in debug mode.")
    asyncio.run(main(), debug=DEBUG)
