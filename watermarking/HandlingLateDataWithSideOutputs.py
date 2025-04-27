from pyflink.common import Types, WatermarkStrategy, Time, Duration
# Correct the import path for OutputTag

from pyflink.datastream.output_tag import OutputTag
from pyflink.common.watermark_strategy import TimestampAssigner
# You might need ReduceFunction if you change .sum(1) later like in previous examples
from pyflink.datastream.functions import ReduceFunction
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows

# ... rest of the code ...

"""
**3. Handling Late Data (`handling_late_data.py`)**
- What happens if data arrives _so_ late that the watermark has already passed the end of the window it belonged to? By default, Flink drops this "late data". However, you can capture it using a "side output". **Concept:**
- **How it works:**
    - You define an `OutputTag` - a unique identifier for the late data stream.
    - When defining the window operation, you chain `.side_output_late_data(your_output_tag)`.
    - After the main windowed aggregation, you can get the stream of late elements using `main_result_stream.get_side_output(your_output_tag)`.

- **Why:** This allows you to log late data, send it to a separate analysis pipeline, or potentially update downstream systems, instead of just losing it.

"""
# Re-use the Timestamp Assigner
class SimpleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # Element: (key, value, timestamp_millis)
        return element[2]

# Define a simple ReduceFunction for summing the values (index 1) if needed
# This is here in case you replace .sum(1) later
class SumReducer(ReduceFunction):
    def reduce(self, v1, v2):
        return (v1[0], v1[1] + v2[1], v1[2]) # Keep key and timestamp from v1

def handling_late_data_example():
    """
    Demonstrates how to capture data that arrives too late for its event time window
    using side outputs.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("Handling Late Data with Side Outputs.")

    # Data with explicit timestamps, including one VERY late event
    # Tuple format: (key, value, timestamp_millis)
    data = [
        ('key1', 10, 1000),  # Event time: 1s
        ('key1', 20, 2000),  # Event time: 2s -> MaxTs = 2000, WM = 1500
        ('key2', 8,  3100),   # Event time: 3.1s -> MaxTs = 3100, WM = 2600. Window [0, 2000) closes.
        ('key1', 50, 4000),  # Event time: 4s   -> MaxTs = 4000, WM = 3500. Window [2000, 4000) closes.
        # *** VERY LATE EVENT ***
        ('key1', 5,  1500),   # Event time: 1.5s. Belongs in window [0, 2000), but arrives AFTER WM passed 2000!
        ('key2', 15, 4500),  # Event time: 4.5s -> MaxTs = 4500, WM = 4000.
        ('key1', 60, 6000),  # Event time: 6s   -> MaxTs = 6000, WM = 5500. Window [4000, 6000) closes.
    ]
    print(f"Input Data (Key, Value, Timestamp):\n{data}")
    print("Note the event ('key1', 5, 1500) which should be in window [0, 2000) but arrives late.")


    data_stream = env.from_collection(
        collection=data,
        type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
    )

    # Define the Bounded Out-of-Orderness Strategy (same as before)
    max_delay_millis = 500
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_delay_millis)) \
        .with_timestamp_assigner(SimpleTimestampAssigner())

    stream_with_watermarks = data_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # --- Define Windowing, Late Data Tag, and Aggregation ---

    # Define an OutputTag to identify the late data stream
    # The type MUST match the type of elements going INTO the window
    late_data_tag = OutputTag(
        "late-elements",
        Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
    )
    print(f"Defined OutputTag for late data: {late_data_tag.tag_id}")

    keyed_stream = stream_with_watermarks.key_by(lambda x: x[0], key_type=Types.STRING())

    window_size_seconds = 2
    windowed_stream = keyed_stream.window(
        TumblingEventTimeWindows.of(Time.seconds(window_size_seconds))
    )

    # *** Specify the side output for late data BEFORE the aggregation ***
    # NOTE: Like the previous example, .sum(1) might cause an AttributeError here too.
    # If it does, replace .sum(1) with .reduce(SumReducer())
    aggregated_stream = windowed_stream.side_output_late_data(late_data_tag).reduce(SumReducer())
        # # .sum(1) # Sum values (index 1) for non-late data
        # .reduce(SumReducer()) # Use this if .sum(1) fails

    print(f"Defined Tumbling Event Time Window ({window_size_seconds}s) with side output for late data.")

    # --- Get and Sink Both Streams ---

    # 1. Sink for the main aggregated results (non-late data)
    print("\n--- Main Window Results (Key, SummedValue) ---")
    aggregated_stream.print().name("MainOutput")

    # 2. Get the side output stream containing late elements
    late_stream = aggregated_stream.get_side_output(late_data_tag)
    print("\n--- Captured Late Data Elements (Key, Value, Timestamp) ---")
    late_stream.print().name("LateDataOutput")


    # Execute the Flink job
    job_name = "Handling Late Data Example"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("\nJob execution finished.")
    print("\nObserve that the element ('key1', 5, 1500) appeared in the 'Late Data Elements' output,")
    print("and was NOT included in the sum for the main window [0, 2000) for 'key1'.")


if __name__ == '__main__':
    handling_late_data_example()