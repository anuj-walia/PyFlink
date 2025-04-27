import time
from pyflink.common import Types, WatermarkStrategy, Time
from pyflink.common.watermark_strategy import TimestampAssigner
# Import ReduceFunction
from pyflink.datastream.functions import ReduceFunction
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream.window import TumblingEventTimeWindows



"""
Okay, let's dive into watermarking in PyFlink. This is a crucial concept for handling **Event Time** processing, which deals with _when events actually happened_ rather than when Flink processes them.
**Why Watermarks? The Problem of Out-of-Order Events**
Imagine data coming from various sensors or distributed services. Due to network delays, buffering, or different processing speeds:
- An event that occurred at `10:00:05` might arrive _after_ an event that occurred at `10:00:06`.
- Events from one source might be significantly delayed compared to another.

If we want to perform calculations based on the _actual event time_ (e.g., "calculate the sum of values for every 10 seconds based on when the event occurred"), we run into a problem: **When do we know we've received all events for a specific time window (like 10:00:00 to 10:00:10)?** We can't just wait indefinitely.
**Watermarks: The Solution**
A **Watermark** is a special Flink message flowing through the stream that carries a timestamp `T`. It acts as a signal: "Flink believes that no more regular data events with a timestamp `t <= T` will arrive from this point forward."
- It's a **heuristic**, a calculated guess based on observed event times and configured strategies.
- It allows Flink to make progress with **event time windows**. When a watermark with timestamp `T` arrives at a window operator, the operator knows that any event time window ending at or before `T` (i.e., `window_end_time <= T`) can be considered complete and can be processed (e.g., the sum for that window can be calculated and emitted).

**Contrast with Java 8 Streams:** Java 8 Streams typically operate on finite, in-memory collections where all data is available upfront and ordered as needed. They don't inherently deal with the complexities of unbounded, distributed, out-of-order event streams and don't have a built-in watermarking mechanism because the problem space is different. Watermarking is specific to distributed stream processing systems handling event time.
Let's look at examples.
**1. Setting Up for Event Time & Timestamp Assignment (`event_time_setup.py`)**
- Before using watermarks, we must tell Flink we want to use Event Time and how to extract the actual timestamp from our data elements. Watermarks themselves are generated based on these timestamps. **Concept:**
- **Details:**
    - We use `StreamExecutionEnvironment.set_stream_time_characteristic(TimeCharacteristic.EventTime)`. _(Note: In newer Flink versions, assigning a `WatermarkStrategy` implicitly sets Event Time, making this explicit call less critical, but it's good for clarity)._
    - We need a `TimestampAssigner` to tell Flink which part of our data represents the event timestamp (usually in milliseconds since the epoch).
    - We then use a `WatermarkStrategy` to combine the timestamp assignment with a watermark generation logic. `WatermarkStrategy.no_watermarks()` only assigns timestamps but doesn't generate watermarks, which isn't useful for triggering event time windows but demonstrates the timestamp assignment step.

"""
"""
**Key Changes:**
1. **Import `ReduceFunction`:** Added the necessary import.
2. **Define `SumReducer`:** Created a class that inherits from `ReduceFunction` and implements the `reduce` method to sum the integer values (at index 1) while keeping the key (at index 0).
3. **Use `.reduce()`:** Replaced with `windowed_stream.reduce(SumReducer())`. `windowed_stream.sum(1)`
4. **Corrected Timestamp Assigner:** I also noticed the original was using (the value) instead of (the timestamp) as intended by the comments and data structure. I've corrected this inside the function for clarity in this specific example file. `SimpleTimestampAssigner``element[1]``element[2]``event_time_setup_example`

Now, the code should run without the `AttributeError`. However, as the comments and the example's purpose highlight, you _still_ won't see windowed output printed because the strategy doesn't provide Flink with the signals needed to close event time windows. The next example () correctly shows how adding a proper watermark strategy enables window triggering. `no_watermarks()``BoundedOutOfOrdernessWatermarkStrategyExample.py`

"""
# Define a simple Timestamp Assigner class
class SimpleTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        # Our element is assumed to be (key, value, timestamp_millis)
        return element[2] # Use index 2 for the timestamp

# Define a simple ReduceFunction for summing the values (index 1)
class SumReducer(ReduceFunction):
    def reduce(self, v1, v2):
        # v1: The current accumulated value (or the first element)
        # v2: The next element to incorporate
        # Output: A new accumulated value (must be the same type as input)
        # We keep the key from the first element and sum the integer value (index 1).
        # The timestamp in the result doesn't have a defined meaning here, we can keep v1's.
        return (v1[0], v1[1] + v2[1], v1[2])

def event_time_setup_example():
    """
    Demonstrates setting up event time and assigning timestamps from data.
    Uses 'no_watermarks' strategy initially to show timestamp assignment alone.
    Uses .reduce() for aggregation syntax correctness.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    # Using event time is implicit when assigning a watermark strategy nowadays
    # env.set_stream_time_characteristic(TimeCharacteristic.EventTime) # Still good for clarity if needed
    env.set_parallelism(1)

    print("Using Event Time characteristic.")

    # Prepare some data with explicit timestamps (milliseconds)
    # Tuple format: (key, value, timestamp_millis)
    # Notice the timestamps are slightly out of order (1500 appears after 2000)
    data = [
        ('key1', 10, 1000),
        ('key1', 20, 2000),
        ('key2', 5, 1200),
        ('key1', 30, 1500), # Out of order timestamp
        ('key2', 8, 3100),
        ('key1', 50, 4000),
    ]
    print(f"Input Data (Key, Value, Timestamp):\n{data}")

    data_stream = env.from_collection(
        collection=data,
        type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
    )

    # ** Corrected Timestamp Assigner to use index 2 **
    class CorrectedTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, element, record_timestamp):
            # Element is (key, value, timestamp_millis)
            return element[2] # Use index 2

    watermark_strategy_no_watermarks = WatermarkStrategy \
        .no_watermarks() \
        .with_timestamp_assigner(CorrectedTimestampAssigner()) # Use the corrected assigner

    # Apply the strategy to the stream
    stream_with_timestamps = data_stream.assign_timestamps_and_watermarks(
        watermark_strategy_no_watermarks
    )
    print("Assigned timestamps using 'no_watermarks' strategy (won't trigger windows effectively).")

    try:
        keyed_stream = stream_with_timestamps \
            .key_by(lambda x: x[0], key_type=Types.STRING())

        windowed_stream = keyed_stream \
            .window(TumblingEventTimeWindows.of(Time.seconds(2)))

        # *** Use .reduce() instead of .sum() ***
        # Apply the reducer function to the windowed stream.
        # The output type will be the same as the input type (the tuple).
        result_stream = windowed_stream.reduce(SumReducer())

        print("\nDefined a 2-second Tumbling Event Time Window with .reduce().")
        print("Attempting to print window results (EXPECT NO OUTPUT because 'no_watermarks' strategy doesn't close windows)...")
        result_stream.print()

        print("\nExecuting job...")
        env.execute_async("Event Time Setup (No Watermarks - Reduce)")
        print("Job submitted. Letting it run for a few seconds...")
        time.sleep(5)
        print("Stopping job (if running). Windows likely didn't trigger due to no watermarks.")

    except Exception as e:
        # We might still get other errors, but the AttributeError should be gone.
        print(f"\nAn error occurred: {e}")

    print("\n--- Example Finished ---")
    print("Takeaway: Using .reduce() fixes the syntax error. However, without proper watermarks,")
    print("Flink still doesn't know when event time windows can close, so no results are emitted.")

if __name__ == '__main__':
    event_time_setup_example()