from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


"""
**. Environment & Data Source (`environment_and_source.py`)**
- **Concept:** The is the starting point for any Flink program. It's used to set properties and create data sources. A data source is where your stream begins. `StreamExecutionEnvironment`
- **Java 8 Analogy:** Think of as the initial step before you can call something like `Stream.of()` or `list.stream()`. The `env.from_collection(...)` is very similar to `Stream.of(...)` or `Arrays.stream(...)` – it takes a known collection of items and turns it into a stream. `StreamExecutionEnvironment.get_execution_environment()`
- **Key Difference:** While Java Streams typically operate on finite, in-memory collections, Flink's environment is built for potentially _infinite_ streams (like from Kafka or sensors) and coordinates execution across potentially many machines (a cluster), even though we run it locally here. Flink also requires explicit type information () for its distributed serialization. `type_info`

"""

def basic_datastream_example():
    """
    A basic PyFlink DataStream example.
    """
    # 1. Set up the execution environment
    # Use STREAMING mode for DataStream API
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # Set parallelism to 1 for local execution simplicity
    env.set_parallelism(1)

    # 2. Create a DataStream from a collection of elements
    data_source = env.from_collection(
        collection=[1, 2, 3, 4, 5],
        type_info=Types.INT()  # Define the type of the elements
    )

    # 3. Define a transformation (e.g., multiply each element by 2)
    transformed_stream = data_source.map(lambda x: x * 2, output_type=Types.INT())

    # 4. Define a sink (e.g., print the results to the console)
    transformed_stream.print()

    # 5. Execute the Flink job
    job_name = "basic_datastream_job"
    print(f"Executing Flink job: {job_name}")
    env.execute(job_name)

if __name__ == '__main__':
    basic_datastream_example()