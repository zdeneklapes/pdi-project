import json
import os
from collections import defaultdict
from datetime import datetime
from enum import Enum
from pprint import pprint
from typing import Tuple, List

from pyflink.common import Duration, Types
from pyflink.common import Encoder
from pyflink.common import Row
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    StreamExecutionEnvironment,
    CheckpointingMode,
    FileSystemCheckpointStorage,
    DataStream,
    FlatMapFunction,
)
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction, ProcessAllWindowFunction
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import SlidingProcessingTimeWindows, TumblingEventTimeWindows

from lib.program import Program


class BoundingBox(Enum):
    LAT_MIN = 0
    LAT_MAX = 1
    LNG_MIN = 2
    LNG_MAX = 3


class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        timestamp = value.get_fields_by_names(["lastupdate"])[0]
        # print("timestamp", timestamp , "id", value.get_fields_by_names(["id"])[0])
        return timestamp


def get_sink(program: Program, task_name: str) -> FileSink:
    sink_dir = program.args["output_dir"] / task_name
    sink_dir.mkdir(parents=True, exist_ok=True)
    sink = (
        FileSink.for_row_format(base_path=str(sink_dir), encoder=Encoder.simple_string_encoder())
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix("task3").with_part_suffix(".txt").build())
        .with_rolling_policy(RollingPolicy.default_rolling_policy())
        .build()
    )
    return sink


# def format_whole_row(record: Row):
#     return (
#         f"{record} | "
#         f"vtype: {record['vtype']:>2} | "
#         f"ltype: {record['ltype']:>2} | "
#         f"lat: {record['lat']:>2.4f} | "
#         f"lng: {record['lng']:>2.4f} | "
#         f"bearing: {record['bearing']:>5.1f} | "
#         f"lineid: {record['lineid']:>4} | "
#         f"linename: {record['linename']:>2} | "
#         f"routeid: {record['routeid']:>5} | "
#         f"course: {record['course']:>2} | "
#         f"lf: {record['lf']:>2} | "
#         f"delay: {record['delay']:>4.1f} | "
#         f"laststopid: {record['laststopid']:>5} | "
#         f"finalstopid: {record['finalstopid']:>5} | "
#         f"isinactive: {record['isinactive']:>5} | "
#         f"lastupdate: {record['lastupdate']:>15} | "
#         f"globalid: {record['globalid']:>5}"
#     )
#

def format_whole_row(record: Row):
    """
    Print the formatted data.
    """
    row = record.as_dict()
    _datetime = datetime.fromtimestamp(row["lastupdate"] / 1000).strftime('%Y-%m-%d %H:%M:%S')
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
        f"delay: {row['delay']:>4.1f} | "
        # f"laststopid: {row['laststopid']:>5} | "
        # f"finalstopid: {row['finalstopid']:>5} | "
        # f"isinactive: {row['isinactive']:>5} | "
        f"lastupdate: {_datetime} | "
        # f"globalid: {row['globalid']:>5}"
    )
    return str_stdout


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
    # TODO: is not supported in JVM 21 ???
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
    # Assign timestamps and watermarks using MyTimestampAssigner
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(20))
        .with_timestamp_assigner(MyTimestampAssigner())
    )

    # watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()
    if program.args["mode"] == "batch":
        program.logger.debug("Processing data in BATCH mode.")
        data_source = env.from_source(
            source=FileSource.for_record_stream_format(
                StreamFormat.text_line_format(), program.args["data_dir"].as_posix()
            ).process_static_file_set()
            # .monitor_continuously(Duration.of_seconds(10)) # TODO: Remove
            .build(),
            watermark_strategy=watermark_strategy,
            source_name="FileSource",
        )
    elif program.args["mode"] == "stream":
        program.logger.debug("Processing data in STREAM mode.")
        data_source = env.from_source(
            source=FileSource.for_record_stream_format(
                StreamFormat.text_line_format(), program.args["data_dir"].as_posix()
            )
            .monitor_continuously(Duration.of_seconds(10))
            .build(),
            watermark_strategy=watermark_strategy,
            source_name="FileSource",
        )
    else:
        env.close()
        assert False, f"Invalid mode: {program.args['mode']}"

    return env, data_source


def key_by_id(vehicle: Row):
    """
    Group vehicles by ID.
    """
    _id = vehicle.get_fields_by_names(["id"])[0]
    # _lastupdate = vehicle.get_fields_by_names(["lastupdate"])[0]
    # print("id", _id, "lastupdate", datetime.fromtimestamp(_lastupdate / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    return _id


def filter_out_none(record):
    """
    Filter out None records.
    """
    if record is None:
        return False
    return True


class MyFunction:
    class DelayImprovementFunction(ProcessAllWindowFunction):
        """
        Process function to calculate and sort delay improvements within a window.
        """

        def __init__(self):
            self.vehicle_buffer = defaultdict(list)

        def _get_improvement(self, prev_record, new_record):
            """
            Calculates the delay improvement between two vehicle records.
            """
            old = prev_record.get_fields_by_names(["delay"])[0]
            new = new_record.get_fields_by_names(["delay"])[0]
            return old - new

        def process(self, context, elements: List[Row]):
            """
            Processes each vehicle record, calculates delay improvement, and buffers results.
            """
            # print("len(elements)", len(elements))
            # pprint([{
            #     "id": element.get_fields_by_names(["id"])[0],
            #     "lastupdate": datetime.fromtimestamp(element.get_fields_by_names(["lastupdate"])[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            # } for element in elements])

            # print(f"Processing {len(elements)} new elements")

            CUSTOM_WINDOW_SIZE = 4

            for value in elements:
                vehicle_id = value.get_fields_by_names(["id"])[0]
                self.vehicle_buffer[vehicle_id].append(value)

                if len(self.vehicle_buffer[vehicle_id]) == CUSTOM_WINDOW_SIZE:
                    # take all record where are at least 2 records
                    assert all([isinstance(x, Row) for x in self.vehicle_buffer[vehicle_id]]), "All elements should be Row"

                    def get_key(x):
                        return x.get_fields_by_names(["lastupdate"])[0]

                    # print("=========== Before sort ===========")
                    # for id, elements in self.vehicle_buffer.items():
                    #     pprint({f"{id}":[{
                    #         "id": element.get_fields_by_names(["id"])[0],
                    #         "delay": element.get_fields_by_names(["delay"])[0],
                    #         "lastupdate": datetime.fromtimestamp(element.get_fields_by_names(["lastupdate"])[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    #     } for element in elements]})

                    # sort vehicles in the buffer by lastupdate
                    for vehicles in self.vehicle_buffer.values():
                        vehicles.sort(key=get_key)

                    # print("=========== After sort ===========")
                    # for id, elements in self.vehicle_buffer.items():
                    #     pprint({f"{id}":[{
                    #         "id": element.get_fields_by_names(["id"])[0],
                    #         "delay": element.get_fields_by_names(["delay"])[0],
                    #         "lastupdate": datetime.fromtimestamp(element.get_fields_by_names(["lastupdate"])[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    #     } for element in elements]})

                    records = [
                        {
                            "id": vehicles[0].get_fields_by_names(["id"])[0],
                            "improvement": self._get_improvement(vehicles[0], vehicles[1]),
                            "previous_delay": vehicles[0].get_fields_by_names(["delay"])[0],
                            "current_delay": vehicles[1].get_fields_by_names(["delay"])[0],
                            "lastupdate": vehicles[1].get_fields_by_names(["lastupdate"])[0],
                        }
                        for vehicles in self.vehicle_buffer.values()
                        if len(vehicles) >= 2
                    ]

                    # pop the first element only if there is more than 2 elements
                    for vehicles in self.vehicle_buffer.values():
                        if len(vehicles) >= 2:
                            vehicles.pop(0)

                    # print("=========== After pop ===========")
                    # for id, elements in self.vehicle_buffer.items():
                    #     pprint({f"{id}":[{
                    #         "id": element.get_fields_by_names(["id"])[0],
                    #         "delay": element.get_fields_by_names(["delay"])[0],
                    #         "lastupdate": datetime.fromtimestamp(element.get_fields_by_names(["lastupdate"])[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    #     } for element in elements]})

                    assert all(len(vehicles) >= 1 for vehicles in self.vehicle_buffer.values()), "All vehicles should have at least 1 record here"

                    assert all([isinstance(x, dict) for x in records]), "All elements should be dict"
                    sorted_results = sorted(records, key=lambda x: x["improvement"], reverse=True)

                    # print("=========== Results ===========")
                    # pprint([{
                    #     'id': record["id"],
                    #     'improvement': record["improvement"],
                    #     # 'previous_delay': record["previous_delay"],
                    #     # 'current_delay': record["current_delay"],
                    #     'lastupdate': datetime.fromtimestamp(record["lastupdate"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    # } for record in sorted_results])

                    # print("=========================================================")

                    for record in sorted_results:
                        # print("record", record)
                        yield Row(
                            id=record["id"],
                            improvement=record["improvement"],
                            previous_delay=record["previous_delay"],
                            current_delay=record["current_delay"],
                            lastupdate=record["lastupdate"],
                        )

    class MinMaxDelayAggregateFunction(AggregateFunction):
        """
        Custom aggregate function to calculate the minimum and maximum delay.
        """

        def create_accumulator(self):
            """
            Initializes an accumulator as a tuple of (min_delay, max_delay).
            """
            return float("inf"), float("-inf")

        def add(self, value, accumulator):
            """
            Updates the accumulator with the delay from the current value.
            """
            delay = value.get_fields_by_names(["delay"])[0]
            min_delay, max_delay = accumulator
            return min(min_delay, delay), max(max_delay, delay)

        def get_result(self, accumulator):
            """
            Returns the final min and max delay from the accumulator.
            """
            return accumulator

        def merge(self, acc1, acc2):
            """
            Merges two accumulators.
            """
            return min(acc1[0], acc2[0]), max(acc1[1], acc2[1])

    class MinMaxDelayWindowFunction(ProcessWindowFunction):
        """
        Adds the window's start and end time to the min/max delay results.
        """

        def process(self, key: str, context: ProcessWindowFunction.Context, averages):
            """
            Process the window and yield the min and max delay along with the window's time range.
            """
            min_delay, max_delay = next(iter(averages))
            window_start = datetime.fromtimestamp(context.window().start / 1000).strftime("%Y-%m-%d %H:%M:%S")
            window_end = datetime.fromtimestamp(context.window().end / 1000).strftime("%Y-%m-%d %H:%M:%S")
            yield {
                "min_delay": min_delay,
                "max_delay": max_delay,
                "window_start": window_start,
                "window_end": window_end,
            }

    class MinMaxIntervalFunction(FlatMapFunction):
        """
        Computes the minimum and maximum interval between the last 10 timestamps.
        """

        # history = []

        def open(self, runtime_context: RuntimeContext):
            # Description for history
            history_descriptor = ValueStateDescriptor(
                "history",
                Types.LIST(
                    Types.ROW_NAMED(
                        [
                            "id",
                            "vtype",
                            "ltype",
                            "lat",
                            "lng",
                            "bearing",
                            "lineid",
                            "linename",
                            "routeid",
                            "course",
                            "lf",
                            "delay",
                            "laststopid",
                            "finalstopid",
                            "isinactive",
                            "lastupdate",
                            "globalid",
                        ],
                        [
                            Types.STRING(),
                            Types.INT(),
                            Types.INT(),
                            Types.FLOAT(),
                            Types.FLOAT(),
                            Types.FLOAT(),
                            Types.INT(),
                            Types.STRING(),
                            Types.INT(),
                            Types.STRING(),
                            Types.STRING(),
                            Types.FLOAT(),
                            Types.INT(),
                            Types.INT(),
                            Types.STRING(),
                            Types.LONG(),
                            Types.STRING(),
                        ],
                    )
                ),
            )
            self.history = runtime_context.get_state(history_descriptor)

        def flat_map(self, value):
            """
            Flat map for descriptor values
            :param value:
            :return:
            """
            if self.history.value() is None:
                self.history.update([value])
            else:
                self.history.update(self.history.value() + [value])

            CUSTOM_WINDOW_SIZE = 10

            RECORD_TO_COUNT = 10

            if len(self.history.value()) == CUSTOM_WINDOW_SIZE:
                # print("=========== Before sort ===========")
                # pprint([{
                #     "id": vehicle.get_fields_by_names(["id"])[0],
                #     "lastupdate": datetime.fromtimestamp(vehicle.get_fields_by_names(["lastupdate"])[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                # } for vehicle in self.history.value()])

                # print("len(self.history.value())", len(self.history.value()))
                min_interval = float("inf")
                max_interval = float("-inf")

                # sort by lastupdate
                self.history.value().sort(key=lambda x: x.get_fields_by_names(["lastupdate"])[0])

                # print("=========== After sort ===========")
                # pprint([{
                #     "id": vehicle.get_fields_by_names(["id"])[0],
                #     "lastupdate": datetime.fromtimestamp(vehicle.get_fields_by_names(["lastupdate"])[0] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                # } for vehicle in self.history.value()])

                first_10_records = self.history.value()[:RECORD_TO_COUNT]

                for i in range(1, len(first_10_records)):
                    interval = abs(
                            self.history.value()[i - 1].get_fields_by_names(["lastupdate"])[0]
                            -
                            self.history.value()[i].get_fields_by_names(["lastupdate"])[0]
                    )
                    min_interval = min(min_interval, interval)
                    max_interval = max(max_interval, interval)
                self.history.value().pop(0)
                # print("len(self.history.value())", len(self.history.value()))
                yield {
                    "id": value.get_fields_by_names(["id"])[0],
                    "datetime_from": self.history.value()[0].get_fields_by_names(["lastupdate"])[0],
                    "datetime_to": value.get_fields_by_names(["lastupdate"])[0],
                    "min_interval": min_interval,
                    "max_interval": max_interval,
                }
            else:
                yield None


class MyTask:
    @staticmethod
    def preprocess_data(data_source: DataStream, program: Program) -> DataStream:
        """
        Preprocess the data before executing the tasks.
        """
        program.logger.debug("Preprocessing the data.")

        # def json_to_dict(json_record):
        #     """
        #     Convert a JSON record to a dictionary.
        #     """
        #     try:
        #         return json.loads(json_record)
        #     except json.JSONDecodeError as e:
        #         program.logger.warning(f"Failed to parse JSON record: {json_record}. Error: {e}")
        #         return None

        def dict_to_row(record):
            """
            Convert dictionary to a flat Flink Row object for proper processing.
            Includes flattening geometry and attributes into top-level fields.
            """
            record = json.loads(record)
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
        # data_source = (
        #     data_source
        #     .map(json_to_dict, output_type=Types.MAP(Types.STRING(), Types.STRING()))
        #     .filter(lambda record: record is not None)
        # )

        output_type = Types.ROW_NAMED(
            [
                "id",
                "vtype",
                "ltype",
                "lat",
                "lng",
                "bearing",
                "lineid",
                "linename",
                "routeid",
                "course",
                "lf",
                "delay",
                "laststopid",
                "finalstopid",
                "isinactive",
                "lastupdate",
                "globalid",
            ],
            [
                Types.STRING(),
                Types.INT(),
                Types.INT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.FLOAT(),
                Types.INT(),
                Types.STRING(),
                Types.INT(),
                Types.STRING(),
                Types.STRING(),
                Types.FLOAT(),
                Types.INT(),
                Types.INT(),
                Types.STRING(),
                Types.LONG(),
                Types.STRING(),
            ],
        )

        def filter_out_inactive(record: Row):
            """
            Filter out inactive records.
            """
            # print("record", record)
            if record is None:
                return False

            if record.get_fields_by_names(["isinactive"])[0].lower() == "true":
                return False

            return True

        # Flatten and structure records into Rows
        data_source = (
            data_source
            .map(dict_to_row, output_type=output_type)
            .filter(filter_out_inactive)
        )

        # PRINT
        # data_source.map(format_whole_row, output_type=Types.STRING()).print()

        program.logger.debug("Preprocessing completed and data has been written to the sink.")

        return data_source

    @staticmethod
    def task_1(data_source: DataStream, program: Program):
        """
        Task 1: Print vehicles in the specified bounding box and save the results.
        """
        program.logger.debug("Processing setup for: 'Task 1' - Filtering vehicles in the specified bounding box.")
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

        new_data = (
            data_source
            .key_by(key_by_id)
            .filter(is_within_bounding_box)
        )

        # SINK
        sink = get_sink(program, "task1")
        new_data.sink_to(sink)

        # PRINT
        _ = (
            new_data
            .map(format_whole_row, output_type=Types.STRING())
            .print()
        )

    @staticmethod
    def task_2(data_source: DataStream, program: Program):
        """
        Task 2: List trolleybuses at their final stop, with stop ID and time of arrival.
        """
        program.logger.debug("Processing setup for: 'Task 2' - Filtering trolleybuses at final stop.")

        def is_trolleybus_at_final_stop(vehicle: Row):
            """
            Check if a trolleybus has reached its final stop.
            """
            row = vehicle.as_dict()
            vtype = row.get("vtype", None)  # Assuming vtype indicates the vehicle type
            last_stop = row.get("laststopid", None)
            final_stop = row.get("finalstopid", None)

            # Ensure the required fields are present
            assert vtype is not None and last_stop is not None and final_stop is not None, f"Invalid vehicle data: {row}"

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
        filtered_data = (
            data_source
            .key_by(key_by_id)
            .filter(is_trolleybus_at_final_stop)
        )

        # SINK
        sink = get_sink(program, "task2")
        filtered_data.sink_to(sink)

        # PRINT
        (
            filtered_data
            .map(format_trolleybus_data, output_type=Types.STRING())
            .print()
        )

    @staticmethod
    def task_3(data_source: DataStream, program: Program):
        """
        Task 3: List delayed vehicles reducing delay, sorted by improvement.
        """
        program.logger.debug("Processing setup for: 'Task 3' - Calculating delayed vehicles reducing delay.")

        # Assign timestamps and watermarks for event-time processing
        watermark_strategy = (
            WatermarkStrategy
            .for_bounded_out_of_orderness(Duration.of_seconds(20))
            .with_timestamp_assigner(MyTimestampAssigner())
        )

        output_type = Types.ROW_NAMED(
            ["id", "improvement", "previous_delay", "current_delay", "lastupdate"],
            [Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.LONG()]
        )

        def filter_out_negative_improvements(record):
            """
            Filter out negative improment records.
            """
            # print("record", record)
            if record["improvement"] <= 0:
                return False
            return True

        processed_data = (
            data_source
            .assign_timestamps_and_watermarks(watermark_strategy)
            # .key_by(key_by_id)  # Group by vehicle ID

            # DEBUG
            # .map(lambda x: {
            #     "id": x.get_fields_by_names(["id"])[0],
            #     "improvement": 0.0,
            #     "previous_delay": 0.0,
            #     "current_delay": 0.0,
            #     "lastupdate": x.get_fields_by_names(["lastupdate"])[0],
            # })
            # DEBUG end

            .window_all(TumblingEventTimeWindows.of(Time.seconds(10)))  # 10-second windows
            .process(MyFunction.DelayImprovementFunction(), output_type=output_type)

            # Only positive improvements
            .filter(filter_out_negative_improvements)
        )

        # Define a sink
        sink = get_sink(program, "task3")
        processed_data.sink_to(sink)

        def format_result(record: dict):
            """
            Format the results for logging or printing.
            """
            return (
                f"ID: {record['id']} | "
                f"improvement: {record['improvement']:.2f} | "
                f"previous Delay: {record['previous_delay']:.2f} | "
                f"current Delay: {record['current_delay']:.2f} | "
                f"lastupdate: {datetime.fromtimestamp(record['lastupdate'] / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
            )

        # Print formatted results
        processed_data.map(format_result, output_type=Types.STRING()).print()

    @staticmethod
    def task_4(data_source: DataStream, program: Program):
        """
        Task 4: Min/max delay in the last 3 minutes.
        """
        program.logger.debug("Processing setup for: 'Task 4' - Calculating min/max delay in the last 3 minutes.")

        # Assign watermarks for event-time processing
        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(10)
        ).with_timestamp_assigner(MyTimestampAssigner())

        # Filter delayed vehicles and calculate min/max delays
        min_max_delays = (
            data_source
            .filter(lambda vehicle: vehicle.get_fields_by_names(["delay"])[0] > 0)
            .assign_timestamps_and_watermarks(watermark_strategy)
            .key_by(lambda vehicle: "all")  # Aggregate across all records
            .window(SlidingProcessingTimeWindows.of(Time.minutes(3), Time.seconds(10)))
            .aggregate(
                MyFunction.MinMaxDelayAggregateFunction(),
                window_function=MyFunction.MinMaxDelayWindowFunction(),
                accumulator_type=Types.TUPLE([Types.FLOAT(), Types.FLOAT()]),
                output_type=Types.MAP(Types.STRING(), Types.STRING()),
            )
        )

        # Define a sink to store results
        sink = get_sink(program, "task4")
        min_max_delays.sink_to(sink)

        def formatted(record: dict):
            """
            Format the results for logging or printing.
            """
            return (
                f"Window Start: {record['window_start']} | "
                f"Window End: {record['window_end']} | "
                f"Min Delay: {record['min_delay']:>5.2f} | "
                f"Max Delay: {record['max_delay']:>5.2f}"
            )

        # Print formatted results
        min_max_delays.map(formatted, output_type=Types.STRING()).print()

        program.logger.debug("Task 4 setup completed.")

    @staticmethod
    def task_5(data_source: DataStream, program: Program) -> DataStream:
        """
        Task 5: Min/max interval of the last 10 updates.
        """
        program.logger.debug("Processing setup for: 'Task 5' - Calculating min/max intervals of the last 10 updates.")

        # Key by vehicle ID to maintain separate state for each vehicle
        keyed_vehicles = data_source.key_by(lambda vehicle: vehicle.get_fields_by_names(["id"])[0])

        # Apply process function to compute intervals
        min_max_intervals = keyed_vehicles.flat_map(MyFunction.MinMaxIntervalFunction())

        # Filter out None records
        min_max_intervals = min_max_intervals.filter(filter_out_none)

        # SINK
        sink = get_sink(program, "task5")
        min_max_intervals.sink_to(sink)

        def formatted(record: dict):
            """
            Format the min/max interval results for logging or saving.
            """
            # program.logger.debug(f"Record: {record}")
            return (
                f"ID: {record['id']:>6} | "
                f"From: {datetime.fromtimestamp(record['datetime_from'] / 1000).strftime('%Y-%m-%d %H:%M:%S')} | "
                f"To: {datetime.fromtimestamp(record['datetime_to'] / 1000).strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Min Interval: {record['min_interval'] / 1000:>5.2f} s | "
                f"Max Interval: {record['max_interval'] / 1000:>5.2f} s"
            )

        # PRINT
        formatted_results = min_max_intervals.map(formatted, output_type=Types.STRING())
        formatted_results.print()
        return min_max_intervals


def process_tasks(program: Program):
    """
    Process the specified task based on the program arguments.
    """
    program.logger.debug(f"Setting up task: {program.args['task']}")
    TASKS = {
        1: MyTask.task_1,
        2: MyTask.task_2,
        3: MyTask.task_3,
        4: MyTask.task_4,
        5: MyTask.task_5,
    }

    # Get the data source
    env, data_source = get_data_source(program, settings={})

    # Preprocess the data
    data_source = MyTask.preprocess_data(data_source, program)

    # try:
    data_source = TASKS[program.args["task"]](data_source, program)
    # except Exception as e:
    #     program.logger.warning(f"Error in task {program.args['task']}: {e}")
    #     exit(1)

    # Execute the Flink job
    program.logger.debug("Processing executed.")
    env.execute("Public Transit Stream Processing")
    env.close()
    program.logger.debug("Flink job completed.")
