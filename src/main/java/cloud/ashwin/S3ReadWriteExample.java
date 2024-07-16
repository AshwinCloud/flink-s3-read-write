package cloud.ashwin;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class S3ReadWriteExample {
    public static void main(String[] args) throws Exception {
//        String s3Bucket = "flink-999888";
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure S3 access (make sure you have the necessary AWS credentials set up)
        env.setParallelism(1);

        // Read from S3
        String inputPath = "s3://flink-999888/input.txt";
        DataStream<String> inputStream = env.readTextFile(inputPath);

        // Process the data (for this example, we'll just convert to uppercase)
        DataStream<String> processedStream = inputStream.map(String::toUpperCase);

        // Write to S3
        String outputPath = "s3://flink-999888/output";
        StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 128) // 128 MB
                                .build())
                .build();

        processedStream.addSink(sink);

        // Execute the job
        env.execute("Flink S3 Read and Write Example");
    }
}