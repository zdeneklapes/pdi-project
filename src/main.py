import asyncio
import json
from argparse import ArgumentParser
from dataclasses import dataclass, field
from enum import Enum, StrEnum

from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode, ExternalizedCheckpointRetention
from pyflink.table import StreamTableEnvironment
from websockets import connect

CONFIGURATION = {
    "uri": "wss://gis.brno.cz/geoevent/ws/services/ODAE_public_transit_stream/StreamServer/subscribe?outSR=4326",
}

ENVIRONMENT = {
    "PRINT": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
}


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

    def debug(self, message):
        if "DEBUG" in ENVIRONMENT["PRINT"]:
            print(f"DEBUG: {message}")

    def info(self, message):
        if "INFO" in ENVIRONMENT["PRINT"]:
            print(f"INFO: {message}")

    def warning(self, message):
        if "WARNING" in ENVIRONMENT["PRINT"]:
            print(f"WARNING: {message}")

    def _assert(self, condition, message):
        if not condition:
            assert False, message



logger = Logger()


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

async def get_flink_env(message_store: list, program: Program, settings: dict):
    """
    Get the Flink environment.
    :param message_store:
    :param program:
    :param settings:
    :return:
    """
    # logger.debug("New message received", message_store[-1])
    logger.debug("New message received")

    stream_env = StreamExecutionEnvironment.get_execution_environment()
    stream_env.set_parallelism(1)
    env = StreamExecutionEnvironment.get_execution_environment()

    # start a checkpoint every 1000 ms
    env.enable_checkpointing(1000)

    # advanced options:

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
    env.get_checkpoint_config().set_externalized_checkpoint_retention(
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    )

    # enables the unaligned checkpoints
    env.get_checkpoint_config().enable_unaligned_checkpoints()

    # settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()

    table_env = StreamTableEnvironment.create(stream_execution_environment=stream_env, environment_settings=settings)

    source = stream_env.from_collection([message_store[-1]], type_info=Types.STRING())

    #
    # parsed_stream = source.map(lambda x: json.loads(x), output_type=Types.MAP(Types.STRING(), Types.STRING()))
    #
    # filtered_stream = parsed_stream.filter(
    #     lambda vehicle: isVehicleInBoundingBox(vehicle, program),
    # )
    #
    # logger.debug("Printing filtered stream...")
    # filtered_stream.print()
    # logger.debug("Printed filtered stream.")
    #
    # # Task 1
    # vehicles_in_bounding_box = [
    #     message for message in message_store.messages if await isVehicleInBoundingBox(json.loads(message), program)
    # ]
    #
    # logger.debug("Starting stream processing...")
    # stream_env.execute("Public Transit Stream Processor")
    #

    return source

async def task_1():
    """
    Task 1: Print vehicles in a specified area.
    :return:
    """
    pass

async def task_2():
    """
    Task 2: List trolleybuses at their final stop.
    :return:
    """
    pass

async def task_3():
    """
    Task 3: List delayed vehicles reducing delay, sorted by improvement.
    :return:
    """
    pass

async def task_4():
    """
    Task 4: Min/max delay in the last 3 minutes.
    :return:
    """
    pass

async def task_5():
    """
    Task 5: Min/max interval of the last 10 updates.
    :return:
    """
    pass

async def process_task(message_store: list, program: Program):
    if program.args["task"] == 1:
        await task_1()
    elif program.args["task"] == 2:
        await task_2()
    elif program.args["task"] == 3:
        await task_3()
    elif program.args["task"] == 4:
        await task_4()
    elif program.args["task"] == 5:
        await task_5()
    else:
        logger._assert(False, "Invalid task number.")

async def start_data_processing(program: Program):
    vehicles: list = []

    async with connect(CONFIGURATION["uri"]) as websocket:
        logger.info("WebSocket connection established.")
        while True:
            try:
                logger.debug("Waiting for message...")
                raw_message = await websocket.recv()
                logger.debug(f"Received message: {raw_message}")

                vehicles.append(raw_message)
                logger.debug(f"Number of vehicles: {len(vehicles)}")
                logger.debug(f"Last vehicle: {vehicles[-1]}")

                # Process the data
                try:
                    # TODO: here it hangs
                    logger.debug("Processing data...")
                    await asyncio.wait_for(process_task(vehicles, program), timeout=program.args["timeout"])
                except asyncio.TimeoutError:
                    logger.warning("No message received within timeout period.")
                break

                # Debugging feedback
                logger.debug("Processed data successfully.")
            except Exception as e:
                logger.error(f"Error during processing: {e}")
                break  # Optionally break the loop on error for debugging


async def parseArgs() -> dict:
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
        required=True,
        help=(
            """
            Task number (1–5):
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
    return vars(args.parse_args())


async def main() -> None:
    program = Program(await parseArgs())
    logger.debug(f"python3 ./src/main.py --bounding-box " + " ".join(map(str, program.args.get("bounding_box"))))
    logger.debug(f"Program arguments: {program.args}")
    await start_data_processing(program)


if __name__ == "__main__":
    asyncio.run(main(), debug=True if "DEBUG" in ENVIRONMENT["PRINT"] else False)

# Generate WebSocket key using OpenSSL
# ws_key = subprocess.run(
#     ["openssl", "rand", "-base64", "16"],
#     capture_output=True,
#     text=True,
#     check=True
# ).stdout.strip()
# # Load the SSL context
# ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
# ssl_context.verify_mode = ssl.CERT_REQUIRED
# ssl_context.check_hostname = True
# ssl_context.load_default_certs()
#
# # Add additional headers for WebSocket handshake
# headers = {
#     "Upgrade": "websocket",
#     "Sec-WebSocket-Key": ws_key,
#     "Sec-WebSocket-Version": "13",
# }
