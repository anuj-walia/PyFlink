from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode

def environment_and_source_example():
    """
    Demonstrates setting up the Flink environment and creating a basic DataStream source.
    """
    # 1. Set up the Execution Environment
    # This is the entry point for any Flink program (DataStream or Table API).
    env = StreamExecutionEnvironment.get_execution_environment()
    # We'll use STREAMING mode for DataStream API examples.
    # BATCH mode is also available for bounded datasets.
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # For local testing, setting parallelism to 1 makes output easier to follow.
    # In a real cluster, this would distribute work.
    env.set_parallelism(1)

    print("Flink Environment created.")

    # 2. Create a DataStream Source
    # This is like Java's Stream.of(1, 2, 3, 4, 5)
    # We create a stream from a simple Python list.
    # Flink needs to know the data type for efficient processing and network transfer.
    data_stream = env.from_collection(
        collection=[1, 2, 3, 4, 5],
        type_info=Types.INT()  # Specify that the elements are integers
    )

    print("DataStream created from collection.")
    print("Stream Type Information:", data_stream.get_type())

    # 3. Define a Sink (where the data goes)
    # .print() is a simple sink that prints each element to the console.
    # This is like Java's stream.forEach(System.out::println).
    data_stream.print()

    # 4. Execute the Flink Job
    # Flink programs are lazy. Operations are defined, but nothing runs until execute() is called.
    # This is similar to how Java Streams only execute when a terminal operation (like forEach, collect) is called.
    job_name = "Environment and Source Example"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("Job execution finished.")

if __name__ == '__main__':
    environment_and_source_example()