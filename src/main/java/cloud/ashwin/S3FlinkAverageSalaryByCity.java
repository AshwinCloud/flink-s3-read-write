package cloud.ashwin;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.java.io.TextInputFormat;

import java.util.concurrent.TimeUnit;

public class S3FlinkAverageSalaryByCity {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define the input and output paths
        String inputPath = "s3://your-input-bucket/input-file.csv";
        String outputPath = "s3://your-output-bucket/output-folder/";

        // Create a TextInputFormat for reading from S3
        TextInputFormat format = new TextInputFormat(new Path(inputPath));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        // Create a DataStream from the source
        DataStream<String> inputStream = env.readFile(format, inputPath);

        // Process the data: filter, map to (city, salary, count), reduce to get sum and count, then calculate average
        DataStream<String> averageSalaryStream = inputStream
            .map(new CsvParser())
            .filter(new JacksonvilleFilter())
            .map(new CityToSalaryMapper())
            .keyBy(tuple -> tuple.f0) // Key by city
            .reduce(new SalaryReducer())
            .map(new AverageSalaryCalculator());

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

        // Add the sink to the processed stream
        averageSalaryStream.addSink(sink);

        // Execute the Flink job
        env.execute("S3 Flink Average Salary by City Job");
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

    // Map to (City, Salary, Count)
    public static class CityToSalaryMapper implements MapFunction<String[], Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> map(String[] value) throws Exception {
            return new Tuple3<>(value[3], Double.parseDouble(value[4]), 1L);
        }
    }

    // Reduce to sum salaries and count
    public static class SalaryReducer implements ReduceFunction<Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> val1, Tuple3<String, Double, Long> val2) throws Exception {
            return new Tuple3<>(val1.f0, val1.f1 + val2.f1, val1.f2 + val2.f2);
        }
    }

    // Calculate average salary
    public static class AverageSalaryCalculator implements MapFunction<Tuple3<String, Double, Long>, String> {
        @Override
        public String map(Tuple3<String, Double, Long> value) throws Exception {
            double averageSalary = value.f1 / value.f2;
            return String.format("%s,%.2f,%d", value.f0, averageSalary, value.f2);
        }
    }
}