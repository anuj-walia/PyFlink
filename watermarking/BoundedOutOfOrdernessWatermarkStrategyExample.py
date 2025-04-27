from pyflink.common import Types, WatermarkStrategy, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows



"""
**2. Bounded Out-of-Orderness Watermark Strategy (`bounded_out_of_orderness.py`)**
- This is the most common and practical strategy. It assumes that events might arrive out of order, but only up to a _maximum delay_ (bound). **Concept:**
- **How it works:** Flink tracks the maximum event timestamp (`max_ts`) seen so far in the stream (per parallel instance). The generated watermark's timestamp (`wm_ts`) is calculated as `wm_ts = max_ts - max_delay`.
- **Example:** If `max_delay` is 500 milliseconds, and the highest event timestamp seen is `2000`, the watermark emitted will be `1500`. This means Flink assumes no more events with timestamp `<= 1500` will arrive. This delay gives late events (like the one with timestamp `1500` arriving after `2000` in our data) a chance to arrive before their corresponding window closes.
- **API:** `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(max_delay))`


_(Expected output for the bounded out-of-orderness example will show sums calculated per key for each 2-second event time window, e.g., `('key1', 40)` for window `[0, 2000)`, `('key2', 5)` for window `[0, 2000)`, `('key2', 8)` for `[2000, 4000)`, `('key1', 50)` for `[2000, 4000)`, etc. The exact print order depends on Flink's internal scheduling, but the grouping by window will be correct due to watermarks.)_

"""


# Re-use the Timestamp Assigner from the previous example
class SimpleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # Element: (key, value, timestamp_millis)
        return element[2] # Index 2 holds the timestamp

def bounded_out_of_orderness_example():
    """
    Demonstrates the Bounded Out-of-Orderness watermark strategy.
    This allows Flink to handle events that arrive slightly late based on their event time.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    # Using event time is implicit when assigning a watermark strategy
    env.set_parallelism(1)

    print("Using Bounded Out-of-Orderness Watermark Strategy.")

    # Data with explicit timestamps (milliseconds), including out-of-order
    # Tuple format: (key, value, timestamp_millis)
    data = [
        ('key1', 10, 1000),  # Event time: 1s
        ('key1', 20, 2000),  # Event time: 2s -> MaxTs = 2000, WM = 2000 - 500 = 1500
        ('key2', 5,  1200),   # Event time: 1.2s (In order wrt WM=1500) -> MaxTs = 2000, WM = 1500
        ('key1', 30, 1500),  # Event time: 1.5s (Out of order, but <= MaxTs=2000. Arrives before WM passes 2000. Included in window [0,2000)!) -> MaxTs = 2000, WM = 1500
        ('key2', 8,  3100),   # Event time: 3.1s -> MaxTs = 3100, WM = 3100 - 500 = 2600. **Window [0, 2000) can now close!**
        ('key1', 50, 4000),  # Event time: 4s   -> MaxTs = 4000, WM = 4000 - 500 = 3500. **Window [2000, 4000) can now close!**
        ('key2', 15, 4500),  # Event time: 4.5s -> MaxTs = 4500, WM = 4500 - 500 = 4000.
        ('key1', 60, 6000),  # Event time: 6s   -> MaxTs = 6000, WM = 6000 - 500 = 5500. **Window [4000, 6000) can now close!**
    ]
    print(f"Input Data (Key, Value, Timestamp):\n{data}")

    data_stream = env.from_collection(
        collection=data,
        type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
    )

    # *** Define the Bounded Out-of-Orderness Strategy ***
    # Allow events to be at most 500 milliseconds late.
    max_delay_millis = 500
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_delay_millis)) \
        .with_timestamp_assigner(SimpleTimestampAssigner()) # Tell it how to get timestamps

    print(f"Defined strategy with max out-of-orderness: {max_delay_millis} ms.")

    # Apply the strategy to the stream
    stream_with_watermarks = data_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # --- Define Windowing and Aggregation ---

    # 1. Key the stream (windows are typically applied on keyed streams)
    keyed_stream = stream_with_watermarks.key_by(lambda x: x[0], key_type=Types.STRING())

    # 2. Define Tumbling Event Time Windows
    # Windows of 2 seconds based on the event timestamp extracted earlier.
    window_size_seconds = 2
    windowed_stream = keyed_stream.window(
        TumblingEventTimeWindows.of(Time.seconds(window_size_seconds))
    )
    print(f"Defined Tumbling Event Time Window of {window_size_seconds} seconds.")

    # 3. Apply aggregation within the window (sum the values - index 1)
    # The sum function here will be triggered *when the watermark passes the window end*.
    result_stream = windowed_stream.sum(1)
    print("Defined sum aggregation per key per window.")

    # Sink: Print the results
    print("\n--- Window Results (Key, SummedValue) ---")
    print("Output appears when watermark allows a window to close.")
    result_stream.print()

    # Execute the Flink job
    job_name = "Bounded Out-of-Orderness Example"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("\nJob execution finished.")
    print("\nObserve how the out-of-order element ('key1', 30, 1500) was correctly included in the first window [0-2000ms) for 'key1'.")
    print("The results for a window are emitted only after the watermark passes the window's end timestamp.")


if __name__ == '__main__':
    bounded_out_of_orderness_example()