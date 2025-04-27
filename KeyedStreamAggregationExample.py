from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


"""
**4. Keyed Streams & Aggregation (`keyed_streams_aggregation.py`)**
- **Concept:** This is where Flink starts to differ significantly from basic Java 8 Streams. `key_by` partitions the DataStream based on a key. All elements with the same key are guaranteed to be processed by the _same_ task (worker). This allows for _stateful_ operations like summing counts per key, because Flink can maintain state (like the current sum) for each key independently. `sum`, `reduce`, `aggregate` are common stateful aggregations performed on these _keyed streams_.
- **Java 8 Analogy:** `key_by` is conceptually similar to Java's `Collectors.groupingBy()`. Performing a `sum(index)` after `key_by` is like `Collectors.groupingBy(keyMapper, Collectors.summingInt(valueMapper))`.
- **Key Difference:** In Java, `groupingBy` typically happens in memory on one machine. In Flink, `key_by` physically shuffles data across the network in a cluster so that all records for a specific key land on the same machine/task. This partitioning is fundamental to Flink's ability to perform distributed, stateful computations and maintain consistency. Flink manages the state reliably, even if machines fail (fault tolerance).


_(Note: The output for the keyed stream example will show intermediate sums as each element arrives, e.g., ('hello', 1), then ('hello', 2), then ('hello', 3).)_

"""


def keyed_streams_aggregation_example():
    """
    Demonstrates keying a stream and performing a stateful aggregation (sum).
    This is fundamental for counting words, aggregating sensor data per sensor, etc.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Parallelism > 1 would show distribution, but 1 is simpler for output order.
    env.set_parallelism(1)

    print("Setting up data source...")
    # Source: A stream of (word, count) tuples. Imagine these come from the FlatMap example.
    data_stream = env.from_collection(
        collection=[
            ('hello', 1), ('world', 1), ('hello', 1), ('flink', 1),
            ('is', 1), ('cool', 1), ('flink', 1), ('hello', 1)
        ],
        # Define the type as a Tuple of (String, Integer)
        type_info=Types.TUPLE([Types.STRING(), Types.INT()])
    )

    # 1. KeyBy Transformation
    # Partition the stream based on a key. Here, the key is the word (the first element, index 0).
    # All tuples with the same word will go to the same processing task.
    # This is crucial for stateful operations like summing counts per word.
    # Analogous to Java's Collectors.groupingBy(tuple -> tuple[0])
    keyed_stream = data_stream.key_by(lambda x: x[0], key_type=Types.STRING())
    print("Applied 'key_by' transformation (grouping by word).")

    # 2. Aggregation on Keyed Stream
    # Perform an aggregation on the keyed stream. Here, we sum the counts (the second element, index 1) for each key (word).
    # Flink maintains the running sum state for each unique key encountered.
    # Analogous to Collectors.groupingBy(..., Collectors.summingInt(tuple -> tuple[1]))
    word_counts_stream = keyed_stream.sum(1) # Sum the element at index 1
    print("Applied 'sum' aggregation on the keyed stream.")

    # Sink: Print the running counts
    # Note: In streaming, this will print the updated count *every time* a new element for that key arrives.
    print("\nRunning word counts:")
    word_counts_stream.print()

    # Execute the job
    job_name = "Keyed Stream Aggregation Example"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("Job execution finished.")

if __name__ == '__main__':
    keyed_streams_aggregation_example()