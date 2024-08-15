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
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputPath = "s3://your-input-bucket/input-file.csv";
        String outputPath = "s3://your-output-bucket/output-folder/";

        TextInputFormat format = new TextInputFormat(new Path(inputPath));
        format.setFilesFilter(FilePathFilter.createDefaultFilter());

        DataStream<String> inputStream = env.readFile(format, inputPath);

        DataStream<String> averageSalaryStream = inputStream
                .map(new CsvParser())
                .filter(new HeaderAndJacksonvilleFilter())
                .map(new CityToSalaryMapper())
                .keyBy(tuple -> tuple.f0)
                .reduce(new SalaryReducer())
                .map(new AverageSalaryCalculator());

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        averageSalaryStream.addSink(sink);

        env.execute("S3 Flink Average Salary by City Job");
    }

    public static class CsvParser implements MapFunction<String, String[]> {
        @Override
        public String[] map(String value) {
            return value.split(",");
        }
    }

    public static class HeaderAndJacksonvilleFilter implements FilterFunction<String[]> {
        @Override
        public boolean filter(String[] value) {
            // Skip the header row and Jacksonville
            return value.length > 3 && !value[3].equals("Jacksonville") && !value[3].equals("City");
        }
    }

    public static class CityToSalaryMapper implements MapFunction<String[], Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> map(String[] value) {
            return new Tuple3<>(value[3], Double.parseDouble(value[4]), 1L);
        }
    }

    public static class SalaryReducer implements ReduceFunction<Tuple3<String, Double, Long>> {
        @Override
        public Tuple3<String, Double, Long> reduce(Tuple3<String, Double, Long> val1, Tuple3<String, Double, Long> val2) {
            return new Tuple3<>(val1.f0, val1.f1 + val2.f1, val1.f2 + val2.f2);
        }
    }

    public static class AverageSalaryCalculator implements MapFunction<Tuple3<String, Double, Long>, String> {
        @Override
        public String map(Tuple3<String, Double, Long> value) {
            double averageSalary = value.f1 / value.f2;
            return String.format("%s,%.2f,%d", value.f0, averageSalary, value.f2);
        }
    }
}