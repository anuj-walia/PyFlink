from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time


"""
**5. Windowing (`windowing_example.py`)**
- **Concept:** Windowing groups stream elements based on time (event time or processing time) or count. Aggregations are then performed _per window_ rather than just per key indefinitely. This is essential for analyzing data over specific periods (e.g., clicks per minute, average temperature per hour). Tumbling windows are fixed-size, non-overlapping windows.
- **Java 8 Analogy:** Basic Java 8 Streams don't have a built-in concept of time-based or count-based windowing for aggregation like Flink does. You'd have to implement complex logic manually, likely collecting elements and then processing them periodically.
- **Key Difference:** Windowing is a core, first-class concept in Flink (and other stream processors) designed for handling continuous, potentially infinite streams where you need to aggregate results over finite chunks. Flink manages the state for each window and triggers computations when windows close.

_(Note: For the windowing example, the output timing depends heavily on how quickly the source emits elements and the 5-second processing time boundary. You might see results appear in batches after pauses.`from_collection`_


"""


def windowing_example():
    """
    Demonstrates windowing: grouping data into finite chunks (windows) based on time
    and performing aggregations per window.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1) # Easier to see window triggers with parallelism 1

    print("Setting up data source...")
    # Source: A stream of (sensor_id, value) tuples.
    # We'll use processing time, so the arrival time at Flink matters.
    # In a real scenario, you might inject delays or use event time timestamps.
    data_stream = env.from_collection(
        collection=[
            ('sensor_1', 10), ('sensor_2', 25), ('sensor_1', 12), # Window 1 likely
            ('sensor_1', 15), ('sensor_2', 30), ('sensor_2', 35), # Window 2 likely
            ('sensor_1', 11),                                    # Window 3 likely
        ],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    # 1. KeyBy Transformation (Group by sensor ID)
    # Windows are usually applied on keyed streams to compute results per key per window.
    keyed_stream = data_stream.key_by(lambda x: x[0], key_type=Types.STRING())
    print("Applied 'key_by' transformation (grouping by sensor_id).")

    # 2. Windowing Transformation
    # Define Tumbling (non-overlapping) windows based on processing time.
    # Each window will cover 5 seconds of processing time.
    # This means elements arriving within the same 5-second interval belong to the same window.
    windowed_stream = keyed_stream.window(
        TumblingProcessingTimeWindows.of(Time.seconds(5))
    )
    print("Applied Tumbling Processing Time Window of 5 seconds.")

    # 3. Aggregation within Window
    # Apply an aggregation (e.g., sum) to the elements within each window for each key.
    # The result is emitted only when the window closes (after 5 seconds of processing time).
    window_sum_stream = windowed_stream.sum(1) # Sum the value (index 1) per sensor_id per window
    print("Applied 'sum' aggregation within each window.")

    # Sink: Print the window results
    # This will print the final sum for each key when its window closes.
    print("\nWindowed sums (results appear after each 5-second window closes):")
    window_sum_stream.print()

    # Execute the job
    job_name = "Windowing Example"
    print(f"\nExecuting Flink job: {job_name} (will run until manually stopped or finishes source)")
    # Note: Processing time windows depend on when data arrives. Run might need a few seconds.
    env.execute(job_name)
    print("Job execution finished.") # May only appear if the source is finite and finishes quickly


if __name__ == '__main__':
    windowing_example()