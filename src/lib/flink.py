import json
import os
from enum import Enum
from typing import Tuple

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
from pyflink.datastream.functions import KeySelector, KeyedProcessFunction

from lib.program import Program


class BoundingBox(Enum):
    LAT_MIN = 0
    LAT_MAX = 1
    LNG_MIN = 2
    LNG_MAX = 3


def get_sink(program: Program, task_name: str) -> FileSink:
    sink_dir = program.args["output_dir"] / task_name
    sink_dir.mkdir(parents=True, exist_ok=True)
    sink = FileSink.for_row_format(
        base_path=str(sink_dir),
        encoder=Encoder.simple_string_encoder()
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("task3")
        .with_part_suffix(".txt")
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ).build()
    return sink


def preprocess_data(data_source: DataStream, program: Program) -> DataStream:
    """
    Preprocess the data before executing the tasks.
    """
    program.logger.debug("Preprocessing the data.")

    def json_to_dict(json_record):
        """
        Convert a JSON record to a dictionary.
        """
        try:
            return json.loads(json_record)
        except json.JSONDecodeError as e:
            program.logger.warning(f"Failed to parse JSON record: {json_record}. Error: {e}")
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
    sink = get_sink(program, "preprocessed")

    # Sink preprocessed data
    data_source.sink_to(sink)

    # formatted_data = data_source.map(
    #     print_all_data_formatted,
    #     output_type=Types.STRING()
    # )
    # formatted_data.print()

    program.logger.debug("Preprocessing completed and data has been written to the sink.")

    return data_source


def get_env(program: Program) -> StreamExecutionEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    cpus = os.cpu_count() // 2 or 1
    program.logger.info(f"Setting parallelism to {cpus}")
    # env.set_parallelism(cpus)
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
    # TODO: is not supported in JVM 17 ???
    # env.get_checkpoint_config().set_externalized_checkpoint_retention(
    #     ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
    # )
    # enables the unaligned checkpoints
    env.get_checkpoint_config().enable_unaligned_checkpoints()

    env.set_buffer_timeout(20000)  # Set buffer timeout to 20 seconds
    # set the checkpoint storage to file system
    checkpoint_dir = program.ROOT_DIR / "tmp/checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    file_storage = FileSystemCheckpointStorage(f"file://{checkpoint_dir}")
    env.get_checkpoint_config().set_checkpoint_storage(file_storage)
    # settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    # table_env = StreamTableEnvironment.create(stream_execution_environment=stream_env, environment_settings=settings)

    return env


def get_data_source(program: Program, settings: dict) -> Tuple[StreamExecutionEnvironment, DataStream]:
    """
    Get the Flink environment.
    :param program:
    :param settings:
    :return:
    """
    env = get_env(program)
    # mode: BULK, BATCH
    program.logger.debug(f"Output directory: {program.args['data_dir']}")
    if program.args["mode"] == "batch":
        program.logger.debug("Processing data in BATCH mode.")
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
        program.logger.debug("Processing data in STREAM mode.")
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


def task_1(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 1: Print vehicles in the specified bounding box and save the results.
    """
    program.logger.debug("Task 1: Filtering vehicles in the specified bounding box.")
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
    formatted_data = data_source.map(
        format_preprocessed_data,
        output_type=Types.STRING()
    )
    formatted_data.print()

    # SINK
    sink = get_sink(program, "task1")
    data_source.sink_to(sink)

    program.logger.debug("Task 1 completed. Results printed and saved.")
    return keyed_data


def task_2(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 2: List trolleybuses at their final stop, with stop ID and time of arrival.
    """
    program.logger.debug("Task 2: Filtering trolleybuses that have reached their final stop.")

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
    sink = get_sink(program, "task2")
    filtered_data.sink_to(sink)

    program.logger.debug("Task 2 completed. Results printed and saved.")
    return filtered_data
    # return data_source


class ReduceDelayProcessFunction(KeyedProcessFunction):
    def __init__(self):
        self.prev_delay_state = None  # State to store the previous delay

    def open(self, runtime_context):
        from pyflink.datastream.state import ValueStateDescriptor
        # Initialize state for storing previous delay
        self.prev_delay_state = runtime_context.get_state(
            ValueStateDescriptor("prev_delay", Types.DOUBLE())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context", out: "Collector"):
        """
        Process each vehicle record to calculate delay improvement.
        """
        current_delay = value["delay"]
        previous_delay = self.prev_delay_state.value()

        if previous_delay is not None and current_delay < previous_delay:
            # Calculate the improvement
            delay_improvement = previous_delay - current_delay
            out.collect({
                "id": value["id"],
                "improvement": delay_improvement,
                "current_delay": current_delay,
                "previous_delay": previous_delay
            })

        # Update the state with the current delay
        self.prev_delay_state.update(current_delay)


def task_3(data_source: DataStream, program: Program) -> DataStream:
    """
    Task 3: List delayed vehicles reducing delay, sorted by improvement.
    """
    program.logger.debug("Processing setup for: 'Task 3' - Calculating delayed vehicles reducing delay.")
    sink = get_sink(program, "task3")

    # return data_source

    # Filter vehicles with non-zero delays
    delayed_vehicles = data_source.filter(lambda vehicle: vehicle["delay"] > 0)

    # Key by vehicle ID to track delay history per vehicle
    keyed_vehicles = delayed_vehicles.key_by(lambda vehicle: vehicle["id"])

    # keyed_vehicles.sink_to(sink)


    # Apply the ReduceDelayProcessFunction
    reduced_delays = keyed_vehicles.process(ReduceDelayProcessFunction())

    sorted_by_improvement = reduced_delays.map(
        lambda record: dumps(record),  # Serialize to JSON
        output_type=Types.STRING()
    ).process(
        lambda values: sorted(values, key=lambda x: x["improvement"], reverse=True)
    )

    # # Define a sink to save the results
    # sink_dir = program.args["output_dir"] / "task3"
    # sink_dir.mkdir(parents=True, exist_ok=True)
    #
    #
    # sorted_by_improvement.sink_to(sink)
    #
    # # Print the results for debugging
    # def format_improvement(record):
    #     return (
    #         f"ID: {record['id']:<10} | "
    #         f"Improvement: {record['improvement']:<10.2f} | "
    #         f"Previous Delay: {record['previous_delay']:<10.2f} | "
    #         f"Current Delay: {record['current_delay']:<10.2f}"
    #     )
    #
    # sorted_by_improvement.map(format_improvement).print()

    program.logger.debug("Task 3 completed. Results printed and saved.")
    # return sorted_by_improvement
    return keyed_vehicles


# def task_4(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 4: Min/max delay in the last 3 minutes.
#     """
#     program.logger.debug("Task 4: Calculating min/max delay in the last 3 minutes.")
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
# def task_5(data_source: DataStream, program: Program) -> DataStream:
#     """
#     Task 5: Min/max interval of the last 10 updates.
#     """
#     program.logger.debug("Task 5: Calculating min/max intervals of the last 10 updates.")
#
#     def map_vehicle_to_key(vehicle):
#         return vehicle.get('attributes', {}).get('vehicleid', None)
#
#     interval_stream = data_source.key_by(
#         lambda vehicle: map_vehicle_to_key(vehicle)
#     ).flat_map(ComputeIntervalsFunction())
#     interval_stream.print()
#     return interval_stream

def process_tasks(program: Program):
    """
    Process the specified task based on the program arguments.
    """
    program.logger.debug(f"Setting up task: {program.args['task']}")
    TASKS = {
        1: task_1,
        2: task_2,
        3: task_3,
        # 4: task_4,
        # 5: task_5,
    }

    # Get the data source
    env, data_source = get_data_source(program, settings={})

    # Preprocess the data
    data_source = preprocess_data(data_source, program)

    # if 0 in program.args["task"]:
    #     # Execute all tasks if task 0 is specified
    #     for task_number, task_function in TASKS.items():
    #         data_source = task_function(data_source, program)
    # else:
    #     # Execute the specified task
    #     for task_number in program.args["task"]:
    #         data_source = TASKS[task_number](data_source, program)

    try:
        data_source = TASKS[program.args["task"]](data_source, program)
    except:
        program.logger.warning(f"Error in task: {program.args['task']}")
        exit(1)

    # Execute the Flink job
    program.logger.debug("Processing executed.")
    env.execute("Public Transit Stream Processing")
    env.close()
    program.logger.debug("Flink job completed.")
