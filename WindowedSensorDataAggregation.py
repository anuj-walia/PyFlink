import time # Import the time module
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.window import TumblingProcessingTimeWindows, Time
from pyflink.datastream.functions import ReduceFunction, MapFunction


"""
**5. Windowing (`windowing_example.py`)**
- **Concept:** Windowing groups stream elements based on time (event time or processing time) or count. Aggregations are then performed _per window_ rather than just per key indefinitely. This is essential for analyzing data over specific periods (e.g., clicks per minute, average temperature per hour). Tumbling windows are fixed-size, non-overlapping windows.
- **Java 8 Analogy:** Basic Java 8 Streams don't have a built-in concept of time-based or count-based windowing for aggregation like Flink does. You'd have to implement complex logic manually, likely collecting elements and then processing them periodically.
- **Key Difference:** Windowing is a core, first-class concept in Flink (and other stream processors) designed for handling continuous, potentially infinite streams where you need to aggregate results over finite chunks. Flink manages the state for each window and triggers computations when windows close.

_(Note: For the windowing example, the output timing depends heavily on how quickly the source emits elements and the 5-second processing time boundary. You might see results appear in batches after pauses.`from_collection`_


"""
class SumReducer(ReduceFunction):
    def reduce(self, v1, v2):
        # v1: The current accumulated value (or the first element)
        # v2: The next element to incorporate
        # Output: A new accumulated value (must be the same type as input)
        # Keep the key from the first element and sum the integer value (index 1).
        # print(type(v1))
        # print(v1)
        # print(type(v2))
        # print(v2)
        return (v1[0],v1[1]+ v2[1]) # Keep key and timestamp from v1 for simplicity

# Add a simple MapFunction to introduce a delay
class DelayMap(MapFunction):
    def map(self, value):
        # Add a small delay to simulate slower data arrival
        time.sleep(1) # Sleep for 1 second per element
        return value

def windowing_example():
    """
    Demonstrates windowing: grouping data into finite chunks (windows) based on time
    and performing aggregations per window.
    Includes an artificial delay to allow processing time windows to trigger with finite source.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1) # Easier to see window triggers with parallelism 1

    print("Setting up data source...")
    # Source: A stream of (sensor_id, value) tuples.
    # We'll use processing time, so the arrival time at Flink matters.
    # In a real scenario, you might inject delays or use event time timestamps.
    data_stream_raw = env.from_collection(
        collection=[
            ('sensor_1', 10), ('sensor_2', 25), ('sensor_1', 12), # Elements 0, 1, 2
            ('sensor_1', 15), ('sensor_2', 30), ('sensor_2', 35), # Elements 3, 4, 5
            ('sensor_1', 11),                                    # Element 6
        ],
        type_info=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    # Introduce an artificial delay using map BEFORE key_by and windowing
    # This will make the source effectively take ~7 seconds to emit all data
    data_stream = data_stream_raw.map(DelayMap(), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
    print("Applied artificial delay via map function.")


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

    window_sum_stream = windowed_stream.reduce(SumReducer())
    # Sum the value (index 1) per sensor_id per window
    print("Applied 'reduce' aggregation within each window.") # Changed from 'sum' to 'reduce' for clarity

    # Sink: Print the window results
    # This will print the final sum for each key when its window closes.
    print("\nWindowed sums (results appear after each 5-second window closes):")
    window_sum_stream.print()

    # Execute the job
    job_name = "Windowing Example"
    print(f"\nExecuting Flink job: {job_name} (will run until manually stopped or finishes source)")
    # Note: Processing time windows depend on when data arrives. Run might need a few seconds.
    env.execute(job_name)
    print("Job execution finished.") # Will appear after source finishes and windows close


if __name__ == '__main__':
    windowing_example()