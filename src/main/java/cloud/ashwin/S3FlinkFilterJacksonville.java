package cloud.ashwin;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;

import java.util.concurrent.TimeUnit;

public class S3FlinkFilterJacksonville {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the input and output paths
        String inputPath = "s3://flink-888999/salary.csv";
        String outputPath = "s3://flink-888999/output-filtered/";

        // Create a TextInputFormat for reading from S3
        TextInputFormat format = new TextInputFormat(new Path(inputPath));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        // Create a DataStream from the source
        DataStream<String> inputStream = env.readFile(format, inputPath);

        // Filter out Jacksonville and convert back to CSV string
        DataStream<String> filteredStream = inputStream
                .map(new CsvParser())
                .filter(new JacksonvilleFilter())
                .map(new ArrayToCsvString());

        // Create a StreamingFileSink for writing to S3
        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024) // 1GB
                                .build())
                .build();

        // Add the sink to the filtered stream
        filteredStream.addSink(sink);

        // Execute the Flink job
        env.execute("S3 Flink Filter Jacksonville Job");
    }

    // Parse CSV line
    public static class CsvParser implements MapFunction<String, String[]> {
        @Override
        public String[] map(String value) throws Exception {
            return value.split(",");
        }
    }

    // Filter to exclude Jacksonville
    public static class JacksonvilleFilter implements FilterFunction<String[]> {
        @Override
        public boolean filter(String[] value) throws Exception {
            return value.length > 3 && !value[3].equals("Jacksonville");
        }
    }

    // Convert String array back to CSV string
    public static class ArrayToCsvString implements MapFunction<String[], String> {
        @Override
        public String map(String[] value) throws Exception {
            return String.join(",", value);
        }
    }
}