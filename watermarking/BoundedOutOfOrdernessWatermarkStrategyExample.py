from pyflink.common import Types, WatermarkStrategy, Time, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
# Import ReduceFunction
from pyflink.datastream.functions import ReduceFunction
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

"""
The error `AttributeError: 'WindowedStream' object has no attribute 'sum'` in confirms that the `.sum(index)` shortcut is not reliably available directly on the `WindowedStream` object returned by `.window()`. `BoundedOutOfOrdernessWatermarkStrategyExample.py`
The most robust way to perform aggregations on a `WindowedStream` is indeed by using the or `.aggregate()` methods. `.reduce()`
Apply the same fix we used before, replacing with using a . `.sum(1)``.reduce()``SumReducer`

"""
"""
1. **Import :`ReduceFunction`** Added the import at the top.
2. **Define :`SumReducer`** Included the class definition. `SumReducer`
3. **Replace with :`.sum(1)``.reduce(SumReducer())`** Modified the aggregation step to use the reducer.

"""

# Re-use the Timestamp Assigner
class SimpleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # Element: (key, value, timestamp_millis)
        return element[2] # Index 2 holds the timestamp

# Define a simple ReduceFunction for summing the values (index 1)
class SumReducer(ReduceFunction):
    def reduce(self, v1, v2):
        # v1: The current accumulated value (or the first element)
        # v2: The next element to incorporate
        # Output: A new accumulated value (must be the same type as input)
        # Keep the key from the first element and sum the integer value (index 1).
        return (v1[0], v1[1] + v2[1], v1[2]) # Keep key and timestamp from v1 for simplicity

def bounded_out_of_orderness_example():
    """
    Demonstrates the Bounded Out-of-Orderness watermark strategy.
    Uses .reduce() for aggregation.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    print("Using Bounded Out-of-Orderness Watermark Strategy.")

    data = [
        ('key1', 10, 1000),
        ('key1', 20, 2000),
        ('key2', 5,  1200),
        ('key1', 30, 1500), # Out of order, but should be included
        ('key2', 8,  3100),
        ('key1', 50, 4000),
        ('key2', 15, 4500),
        ('key1', 60, 6000),
    ]
    print(f"Input Data (Key, Value, Timestamp):\n{data}")

    data_stream = env.from_collection(
        collection=data,
        type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
    )

    max_delay_millis = 500
    watermark_strategy = WatermarkStrategy \
        .for_bounded_out_of_orderness(Duration.of_millis(max_delay_millis)) \
        .with_timestamp_assigner(SimpleTimestampAssigner())

    print(f"Defined strategy with max out-of-orderness: {max_delay_millis} ms.")

    stream_with_watermarks = data_stream.assign_timestamps_and_watermarks(watermark_strategy)

    keyed_stream = stream_with_watermarks.key_by(lambda x: x[0], key_type=Types.STRING())

    window_size_seconds = 2
    windowed_stream = keyed_stream.window(
        TumblingEventTimeWindows.of(Time.seconds(window_size_seconds))
    )
    print(f"Defined Tumbling Event Time Window of {window_size_seconds} seconds.")

    # *** Use .reduce() instead of .sum() ***
    result_stream = windowed_stream.reduce(SumReducer())
    print("Defined aggregation using .reduce() per key per window.")

    print("\n--- Window Results (Key, SummedValue, Timestamp) ---")
    print("Output appears when watermark allows a window to close.")
    result_stream.print()

    job_name = "Bounded Out-of-Orderness Example (Reduce)"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("\nJob execution finished.")
    print("\nObserve how the out-of-order element ('key1', 30, 1500) was correctly included.")
    print("The results for a window are emitted only after the watermark passes the window's end timestamp.")

if __name__ == '__main__':
    bounded_out_of_orderness_example()