from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


"""

**3. FlatMap Transformation (`flatmap_transformation.py`)**
- **Concept:** `flat_map` is a transformation that takes one element and produces zero, one, or _more_ elements. It's useful for operations like splitting sentences into words or expanding a single event into multiple sub-events.
- **Java 8 Analogy:** This is a direct parallel to Java's `stream.flatMap(function)`. The function you provide should return a collection (or iterator/stream in Java) of output elements for each input element, and `flatMap` "flattens" these collections into a single output stream.
- **Key Difference:** No major conceptual difference from Java Streams, other than the distributed execution context in Flink.
"""

def flatmap_transformation_example():
    """
    Demonstrates the flat_map transformation for splitting items into multiple items.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    print("Setting up data source...")
    # Source: A stream of simple sentences
    data_stream = env.from_collection(
        collection=[
            "hello flink world",
            "apache flink is cool",
            "streaming processing engine"
        ],
        type_info=Types.STRING()
    )

    # 1. FlatMap Transformation
    # Split each sentence (string) into individual words (strings).
    # The lambda function takes one sentence and returns a list of words.
    # FlatMap then merges all these lists into a single stream of words.
    # Like Java: stream.flatMap(sentence -> Arrays.stream(sentence.split(" ")))
    # We need to specify the output type of the elements produced (words are strings).
    words_stream = data_stream.flat_map(lambda sentence: sentence.split(" "),
                                          output_type=Types.STRING())
    print("Applied 'flat_map' transformation (splitting sentences into words).")

    # Sink: Print the resulting words
    print("\nFinal results after flat_map:")
    words_stream.print()

    # Execute the job
    job_name = "FlatMap Example"
    print(f"\nExecuting Flink job: {job_name}")
    env.execute(job_name)
    print("Job execution finished.")

if __name__ == '__main__':
    flatmap_transformation_example()