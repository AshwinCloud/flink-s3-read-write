package cloud.ashwin;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;

public class KafkaJsonProcessingJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String inputTopic = "input_topic";
        String outputTopic = "output_topic";
        String kafkaBootstrapServers = "localhost:9092";

        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Read from Kafka
        DataStream<String> kafkaInputStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Parse JSON and convert to Row with debugging
        DataStream<Row> parsedStream = kafkaInputStream.map(json -> {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(json);
            Row row = Row.of(
                    jsonNode.get("key1").asText(),
                    jsonNode.get("key2").asText(),
                    jsonNode.get("key3").asText(),
                    jsonNode.get("quantity").asInt()
            );
            
            // Debug: Print the structure of the row
            System.out.println("Parsed Row: " + row);
            System.out.println("Number of fields: " + row.getArity());
            for (int i = 0; i < row.getArity(); i++) {
                System.out.println("Field " + i + ": " + row.getField(i));
            }
            
            return row;
        }).returns(Row.class);

        // Debug: Print the first few elements of the parsed stream
        parsedStream.print();

        // Create a table from the parsed stream
        Table inputTable = tableEnv.fromDataStream(parsedStream).as("key1", "key2", "key3", "quantity");

        // Apply filter and select
        Table resultTable = tableEnv.sqlQuery(
                "SELECT key1, key2, key3, quantity FROM " + inputTable + " WHERE quantity > 10"
        );

        // Convert the result table back to a DataStream
        DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

        // Convert Row back to JSON string
        DataStream<String> outputStream = resultStream.map(row -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(new HashMap<String, Object>() {{
                put("key1", row.getField(0));
                put("key2", row.getField(1));
                put("key3", row.getField(2));
                put("quantity", row.getField(3));
            }});
        });

        // Configure Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // Write to Kafka
        outputStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Kafka JSON Processing Job");
    }
}