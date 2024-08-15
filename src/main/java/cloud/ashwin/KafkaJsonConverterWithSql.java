package cloud.ashwin;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.table.api.Schema;


public class KafkaJsonConverterWithSql {
//    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
//    rootLogger.setLevel(org.apache.log4j.Level.INFO);

    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonConverterWithSql.class);

    public static void main(String[] args) throws Exception {
        final String inputTopic = "input_topic";
        final String outputTopic = "output_topic";
        final String filteredOutputTopic = "output_topic_filtered";
        final String bootstrapServers = "localhost:9092";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy.forMonotonousTimestamps();

        DataStream<String> inputStream = env.fromSource(source, watermarkStrategy, "Kafka Source");

        DataStream<JsonRecord> jsonStream = inputStream.map(line -> {
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

            JsonRecord record = new JsonRecord(
                    jsonNode.get("key1").asText(),
                    jsonNode.get("key2").asText(),
                    jsonNode.get("key3").asText(),
                    jsonNode.get("quantity").asInt()
            );
            LOG.info("Parsed record: {}", record);
            return record;
        });

        jsonStream = jsonStream.map(record -> {
            System.out.println("Parsed record: " + record);
            record.quantity += 1;
            return record;
        });

        Schema schema = Schema.newBuilder()
                .column("key1", "STRING")
                .column("key2", "STRING")
                .column("key3", "STRING")
                .column("quantity", "INT")
                .build();

        // Create a table from the JSON stream
        Table inputTable = tableEnv.fromDataStream(jsonStream, schema);
        tableEnv.createTemporaryView("input_table", inputTable);
//
        // SQL query to filter records with quantity > 10
        Table filteredTable = tableEnv.sqlQuery(
                "SELECT * FROM input_table where quantity > 10"
        );

        // Convert the filtered table back to a DataStream
        DataStream<JsonRecord> filteredStream = tableEnv.toDataStream(filteredTable, JsonRecord.class);

//        // Add logging to the filtered stream
//        filteredStream = filteredStream.map(record -> {
//            LOG.info("Filtered record: {}", record);
//            return record;
//        });
//
//        filteredStream = filteredStream.map(record -> {
//            System.out.println("Filtered record: " + record);
//            return record;
//        });

        // Kafka sink for all records
        KafkaSink<String> allRecordsSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        // Kafka sink for filtered records
        KafkaSink<String> filteredRecordsSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(filteredOutputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        System.out.println("test printout");
        jsonStream.print();
        // Write all records to Kafka
        jsonStream.map(record -> new ObjectMapper().writeValueAsString(record)).sinkTo(allRecordsSink);

        // Write filtered records to Kafka
        filteredStream.map(record -> new ObjectMapper().writeValueAsString(record)).sinkTo(filteredRecordsSink);

        env.execute("Kafka JSON Converter with SQL Filtering");
    }

    // POJO class to represent the JSON record
    public static class JsonRecord {
        public String key1;
        public String key2;
        public String key3;
        public int quantity;

        public JsonRecord() {}

        public JsonRecord(String key1, String key2, String key3, int quantity) {
            this.key1 = key1;
            this.key2 = key2;
            this.key3 = key3;
            this.quantity = quantity;
        }

        @Override
        public String toString() {
            return String.format("JsonRecord(key1=%s, key2=%s, key3=%s, quantity=%d)", key1, key2, key3, quantity);
        }
    }
}