from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


"""
**2. Basic Transformations: Map & Filter (`basic_transformations.py`)**
- **Concept:** Transformations are operations that take one or more DataStreams and produce a new DataStream. applies a function to each element individually, changing it. `filter` keeps only elements that satisfy a condition. `map`
- **Java 8 Analogy:** These are direct parallels. Flink's is exactly like Java's `stream.map(function)`, and Flink's `filter` is exactly like Java's `stream.filter(predicate)`. They are stateless operations, meaning each element is processed independently of others. `map`
- **Key Difference:** Again, Flink applies these potentially across a distributed cluster, handling data serialization and movement between tasks automatically.

"""

def basic_transformations_example():
    """
    Demonstrates basic stateless transformations: map and filter.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    print("Setting up data source...")
    # Source: A stream of numbers 1 to 10
    data_stream = env.from_collection(
        collection=list(range(1, 11)), # [1, 2, ..., 10]
        type_info=Types.INT()
    )

    # 1. Map Transformation
    # Apply a function to each element. Here, we square each number.
    # Like Java: stream.map(x -> x * x)
    # We need to specify the output type after the map operation.
    squared_stream = data_stream.map(lambda x: x * x, output_type=Types.INT())
    print("Applied 'map' transformation (squaring numbers).")

    # 2. Filter Transformation
    # Keep only elements that satisfy a condition. Here, keep only even squares.
    # Like Java: stream.filter(x -> x % 2 == 0)
    filtered_stream = squared_stream.filter(lambda x: x % 2 == 0)
    # Note: Filter doesn't change the type, so output_type isn't needed here.
    print("Applied 'filter' transformation (keeping even numbers).")

    # Sink: Print the final results
    print("\nFinal results after map and filter:")
    filtered_stream.print()

    # Execute the job
    job_name = "Map and Filter Example"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("Job execution finished.")

if __name__ == '__main__':
    basic_transformations_example()