import asyncio
import os
from argparse import ArgumentParser
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, StrEnum
from pathlib import Path
from typing import Tuple

import aiofile
from pyflink.common import Duration
from pyflink.common import Types, WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, ExternalizedCheckpointRetention, FileSystemCheckpointStorage, DataStream, KeyedProcessFunction, FlatMapFunction
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingProcessingTimeWindows
from websockets import connect

ENVIRONMENT = {
    "PRINT": [
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL"
    ]
}
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
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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


logger = Logger()


class ReduceDelayProcessFunction(KeyedProcessFunction):
    def process_element(self, vehicle, ctx: "KeyedProcessFunction.Context", out):
        prev_delay = self.get_runtime_context().get_state(ValueStateDescriptor("prev_delay", Types.DOUBLE()))
        curr_delay = vehicle[PublicTransitKey.delay]
        if prev_delay.value() is not None and curr_delay < prev_delay.value():
            delay_diff = prev_delay.value() - curr_delay
            out.collect((vehicle[PublicTransitKey.id], delay_diff))
        prev_delay.update(curr_delay)


class ComputeIntervalsFunction(FlatMapFunction):
    def __init__(self):
        self.timestamps = []

    def flat_map(self, vehicle, out):
        self.timestamps.append(vehicle[PublicTransitKey.lastupdate])
        if len(self.timestamps) > 10:
            self.timestamps.pop(0)
        if len(self.timestamps) > 1:
            intervals = [self.timestamps[i] - self.timestamps[i - 1] for i in range(1, len(self.timestamps))]
            out.collect((min(intervals), max(intervals)))


class CustomTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value["attributes"]["lastupdate"]


class MinMaxDelayAggregate(AggregateFunction):
    """
    Aggregate function to calculate the minimum and maximum delays.
    """

    def create_accumulator(self):
        """
        Initializes the accumulator with default min and max values.
        """
        return float("inf"), float("-inf")  # (min_delay, max_delay)

    def add(self, value, accumulator):
        """
        Updates the accumulator with a new delay value.
        :param value: The current vehicle data (expects delay field).
        :param accumulator: Tuple (min_delay, max_delay).
        """
        delay = value[PublicTransitKey.delay]
        min_delay, max_delay = accumulator
        return min(delay, min_delay), max(delay, max_delay)

    def get_result(self, accumulator):
        """
        Returns the min and max delays from the accumulator.
        """
        return accumulator

    def merge(self, acc1, acc2):
        """
        Merges two accumulators.
        """
        return min(acc1[0], acc2[0]), max(acc1[1], acc2[1])


class PublicTransitKey(StrEnum):
    """Data class representing a single entry from the public transit stream."""

    globalid = "globalid"
    id = "id"
    vtype = "vtype"
    ltype = "ltype"
    lat = "lat"
    lng = "lng"
    bearing = "bearing"
    lineid = "lineid"
    linename = "linename"
    routeid = "routeid"
    course = "course"
    lf = "lf"
    delay = "delay"
    laststopid = "laststopid"
    finalstopid = "finalstopid"
    isinactive = "isinactive"
    lastupdate = "lastupdate"


async def isVehicleInBoundingBox(vehicle: dict[str], program: Program) -> bool:
    bounding_box = program.args["bounding_box"]
    isIn = (
            bounding_box[BoundingBox.LAT_MIN.value]
            <= vehicle[PublicTransitKey.lat]
            <= bounding_box[BoundingBox.LAT_MAX.value]
            and bounding_box[BoundingBox.LNG_MIN.value]
            <= vehicle[PublicTransitKey.lng]
            <= bounding_box[BoundingBox.LNG_MAX.value]
    )
    logger.debug(f"Vehicle {vehicle[PublicTransitKey.globalid]} is in bounding box: {isIn}")
    return isIn


async def get_data_source(message_store: list, program: Program, settings: dict) -> Tuple[StreamExecutionEnvironment, DataStream]:
    """
    Get the Flink environment.
    :param message_store:
    :param program:
    :param settings:
    :return:
    """
    # logger.debug("New message received", message_store[-1])
    logger.debug("New message received")
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

    data_source = env.from_collection(
        collection=message_store,
    )
    return env, data_source


async def task_1(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 1: Print vehicles in a specified area.
    """
    logger.debug("Task 1: Filtering vehicles in the specified bounding box.")
    bounding_box = program.args["bounding_box"]
    filtered_data = data_source.filter(
        lambda vehicle: bounding_box[BoundingBox.LAT_MIN.value]
                        <= vehicle[PublicTransitKey.lat]
                        <= bounding_box[BoundingBox.LAT_MAX.value]
                        and bounding_box[BoundingBox.LNG_MIN.value]
                        <= vehicle[PublicTransitKey.lng]
                        <= bounding_box[BoundingBox.LNG_MAX.value]
    )
    filtered_data.print()
    return filtered_data


async def task_2(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 2: List trolleybuses at their final stop.
    """
    logger.debug("Task 2: Filtering trolleybuses at their final stop.")
    trolleybuses_at_final_stop = data_source.filter(
        lambda vehicle: vehicle[PublicTransitKey.ltype] == 2  # Assuming ltype 2 indicates trolleybus
                        and vehicle[PublicTransitKey.laststopid] == vehicle[PublicTransitKey.finalstopid]
    ).map(
        lambda vehicle: {
            "id": vehicle[PublicTransitKey.id],
            "stop": vehicle[PublicTransitKey.laststopid],
            "timestamp": vehicle[PublicTransitKey.lastupdate],
        }
    )
    trolleybuses_at_final_stop.print()
    return trolleybuses_at_final_stop


async def task_3(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 3: List delayed vehicles reducing delay, sorted by improvement.
    """
    logger.debug("Task 3: Finding delayed vehicles reducing delay.")
    vehicles_with_delay = data_source.key_by(lambda vehicle: vehicle[PublicTransitKey.id]).process(
        ReduceDelayProcessFunction()
    )
    vehicles_with_delay.print()
    return vehicles_with_delay


async def task_4(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 4: Min/max delay in the last 3 minutes.
    """
    logger.debug("Task 4: Calculating min/max delay in the last 3 minutes.")
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10)).with_timestamp_assigner(
        CustomTimestampAssigner()
    )
    delay_stream = data_source.assign_timestamps_and_watermarks(watermark_strategy).key_by(
        lambda vehicle: vehicle[PublicTransitKey.id]
    ).window(
        SlidingProcessingTimeWindows.of(Time.minutes(3), Time.seconds(10))
    ).aggregate(
        MinMaxDelayAggregate(),
        output_type=Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()])
    )
    delay_stream.print()
    return delay_stream


async def task_5(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 5: Min/max interval of the last 10 updates.
    """
    logger.debug("Task 5: Calculating min/max intervals of the last 10 updates.")
    interval_stream = data_source.key_by(
        lambda vehicle: vehicle[PublicTransitKey.id]
    ).flat_map(ComputeIntervalsFunction())
    interval_stream.print()
    return interval_stream


async def process_task(message_store: list, program: Program):
    """
    Process the specified task based on the program arguments.
    """
    TASKS = {
        1: task_1,
        2: task_2,
        3: task_3,
        4: task_4,
        5: task_5,
    }
    logger.debug(f"Starting task processing: Task {program.args['task']}.")

    # Get the data source
    env, data_source = await get_data_source(message_store, program, settings={})

    if program.args["task"] == 0:
        # Execute all tasks if task 0 is specified
        logger.debug("Task 0: Executing all tasks.")
        for task_number, task_function in TASKS.items():
            logger.debug(f"Executing Task {task_number}.")
            data_source = await task_function(data_source, program)
    else:
        # Execute the specified task
        task_function = TASKS.get(program.args["task"])
        if task_function:
            logger.debug(f"Executing Task {program.args['task']}.")
            data_source = await task_function(data_source, program)
        else:
            logger.error(f"Invalid task number: {program.args['task']}.")
            raise ValueError(f"Invalid task number: {program.args['task']}.")

    # Execute the Flink job
    logger.debug("Task processing started.")
    env.execute("Public Transit Stream Processing")
    logger.debug("Task processing completed.")



class WebStreamStore:
    URI = "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326"

    def __init__(self, program):
        self.messages = asyncio.Queue()
        self.program = program
        self.websocket = None

    async def __aenter__(self):
        self.connection = connect(self.URI)
        self.timestamp: str = datetime.now().strftime("%Y%m%d%H%M%S")
        self.websocket = await self.connection.__aenter__()
        logger.info(f"Connected to websocket: {self.URI}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.connection.__aexit__(exc_type, exc_val, exc_tb)

    def _get_file_name(self):
        return self.program.args["data_dir"] / f"{self.timestamp}.json"

    async def receive(self):
        """ Receive messages continuously and store them. """
        while True:
            message = await self.websocket.recv()
            await self.messages.put(message)
            await self.save_to_file(message)

    async def save_to_file(self, message):
        async with aiofile.async_open(self._get_file_name(), "a") as f:
            # logger.debug(f"Saving message to file: {self._get_file_name()}")
            await f.write(message)

    async def receive_and_save(self):
        while True:
            message = await self.websocket.recv()
            await self.save_to_file(message)

    def is_file_processed(self):
        try:
            return self.file_processed
        except AttributeError:
            self.file_processed = False
            return self.file_processed

    async def get_messages(self) -> list:
        """ Retrieve all messages and clear the store. """
        _all = []
        if self.program.args["process_data_file"]:
            if self.is_file_processed():
                raise Exception("File already processed.")
            self.file_processed = True
            async with aiofile.async_open(self.program.args["process_data_file"], "r") as file:
                async for chunk in file.iter_chunked(aiofile.Reader.CHUNK_SIZE):
                    _all.append(chunk)
                return _all
        else:
            while not self.messages.empty():
                _all.append(await self.messages.get())
            return _all


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
        default=0,
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
        '--max-messages',
        type=int,
        default=None,
        help="Max number of messages to process.",
    )
    args.add_argument(
        "--output_dir",
        type=str,
        default=ROOT_DIR / "tmp/results",
        help="Output directory for the results.",
    )
    args.add_argument(
        "--checkpoint_dir",
        type=str,
        default=ROOT_DIR / "tmp/checkpoints",
        help="Directory for storing checkpoints.",
    )
    args.add_argument(
        "--data_dir",
        type=str,
        default=ROOT_DIR / "tmp/data",
        help="Directory for storing messages.",
    )
    args.add_argument(
        "--process_data_file",
        type=str,
        default=None,
        help="Process a single file.",
    )
    args.add_argument(
        "--download_data_first",
        action="store_true",
        default=False,
        help="Download data only. Not processing.",
    )

    _args = args.parse_args()

    _args.output_dir = Path(os.path.abspath(_args.output_dir))
    _args.checkpoint_dir = Path(os.path.abspath(_args.checkpoint_dir))
    _args.data_dir = Path(os.path.abspath(_args.data_dir))

    _args.output_dir.mkdir(parents=True, exist_ok=True)
    _args.checkpoint_dir.mkdir(parents=True, exist_ok=True)
    _args.data_dir.mkdir(parents=True, exist_ok=True)

    return vars(_args)


async def process_messages(store: WebStreamStore):
    """ Simulate processing of messages. """
    wait_seconds=4
    while True:
        if store.is_file_processed():
            break
        messages = await store.get_messages()
        if len(messages) > 0:
            logger.info(f"Processing {len(messages)} messages...")
            await process_task(messages, store.program)
        else:
            logger.debug(f"Waiting for messages for {wait_seconds} seconds")
        await asyncio.sleep(wait_seconds)  # Adjust sleep time based on expected message rate
        # sleep(20)


async def main():
    program = Program(parseArgs())
    logger.debug(f"python3 ./src/main.py --bounding-box " + " ".join(map(str, program.args.get("bounding_box"))))
    logger.debug(f"Program arguments: {program.args}")
    async with WebStreamStore(program) as store:
        if program.args["download_data_first"]:
            await store.receive_and_save()
        else:
            receiver_task = asyncio.create_task(store.receive())
            processor_task = asyncio.create_task(process_messages(store))
            await asyncio.wait([receiver_task, processor_task], return_when=asyncio.FIRST_COMPLETED)


if __name__ == "__main__":
    logger.debug("Running in debug mode.")
    asyncio.run(main(), debug=DEBUG)
