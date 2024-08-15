package cloud.ashwin;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class KafkaJsonConverter {

    public static void main(String[] args) throws Exception {
        final String inputTopic = "input_topic";
        final String outputTopic = "output_topic";
        final String bootstrapServers = "localhost:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Use a simple WatermarkStrategy that doesn't actually produce watermarks
        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();

        DataStream<String> inputStream = env.fromSource(source, watermarkStrategy, "Kafka Source");

        DataStream<String> jsonStream = inputStream.map(line -> {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode jsonNode = mapper.createObjectNode();

            String[] keyValuePairs = line.substring(1, line.length() - 1).split(", ");
            for (String pair : keyValuePairs) {
                String[] entry = pair.split(": ");
                String key = entry[0].replace("\"", "");
                String value = entry[1].replace("\"", "");

                if (key.equals("quantity")) {
                    jsonNode.put(key, Integer.parseInt(value));
                } else {
                    jsonNode.put(key, value);
                }
            }

            return mapper.writeValueAsString(jsonNode);
        });

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        jsonStream.sinkTo(sink);

        env.execute("Kafka JSON Converter");
    }
}