package cloud.ashwin;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KafkaJsonSql {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaJsonConverterWithSql2.class);

    private static String preprocessJsonString(String jsonString) {
        // Replace None with null
        jsonString = jsonString.replace("None", "null");

        // Pattern for datetime.datetime(YYYY, M, D, H, M, S)
        Pattern pattern = Pattern.compile("datetime\\.datetime\\((\\d+), (\\d+), (\\d+), (\\d+), (\\d+), (\\d+)\\)");
        Matcher matcher = pattern.matcher(jsonString);

        // Buffer for the resulting string
        StringBuffer resultString = new StringBuffer();

        // Replace datetime.datetime with ISO 8601 date string
        while (matcher.find()) {
            String formattedDate = getFormattedDate(matcher);

            // Append replacement to the result
            matcher.appendReplacement(resultString, formattedDate);
        }
        matcher.appendTail(resultString);

        return resultString.toString();
    }

    private static String getFormattedDate(Matcher matcher) {
        int year = Integer.parseInt(matcher.group(1));
        int month = Integer.parseInt(matcher.group(2));
        int day = Integer.parseInt(matcher.group(3));
        int hour = Integer.parseInt(matcher.group(4));
        int minute = Integer.parseInt(matcher.group(5));
        int second = Integer.parseInt(matcher.group(6));

        LocalDateTime dateTime = LocalDateTime.of(year, month, day, hour, minute, second);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String formattedDate = "\"" + dateTime.format(formatter) + "\"";
        return formattedDate;
    }

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

        DataStream<ObjectNode> jsonStream = inputStream.map(line -> {
            System.out.println("line: " + line);
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode jsonNode = mapper.createObjectNode();

            // Preprocess JSON string
            String jsonString = preprocessJsonString(line);

            try {
                // Parse JSON string into ObjectNode
                jsonNode = (ObjectNode) mapper.readTree(jsonString);

                // Print the ObjectNode
                System.out.println(jsonNode.toPrettyString());

            } catch (Exception e) {
                e.printStackTrace();
            }

            return jsonNode;
        });

        DataStream<KafkaJsonConverterWithSql2.JsonRecord> recordStream = jsonStream.map(jsonNode -> {
            KafkaJsonConverterWithSql2.JsonRecord record = new KafkaJsonConverterWithSql2.JsonRecord(jsonNode.get("key1").asText(), jsonNode.get("key2").asText(), jsonNode.get("key3").asText(), jsonNode.get("quantity").asInt());
            return record;
        });

//        Schema schema = Schema.newBuilder()
//                .column("key1", "STRING")
//                .column("key2", "STRING")
//                .column("key3", "STRING")
//                .column("quantity", "INT")
//                .build();

        // Create a table from the JSON stream
        Table inputTable = tableEnv.fromDataStream(recordStream);

        tableEnv.createTemporaryView("input_table", inputTable);

        // SQL query to filter records with quantity > 10
        Table filteredTable = tableEnv.sqlQuery(
//                "SELECT * FROM input_table"
                "SELECT * FROM input_table where quantity > 10"
        );

        // Convert the filtered table back to a DataStream
        DataStream<KafkaJsonConverterWithSql2.JsonRecord> filteredStream = tableEnv.toDataStream(filteredTable, KafkaJsonConverterWithSql2.JsonRecord.class);

        // Kafka sink for all records
        KafkaSink<String> allRecordsSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

//         Kafka sink for filtered records
        KafkaSink<String> filteredRecordsSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(filteredOutputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

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