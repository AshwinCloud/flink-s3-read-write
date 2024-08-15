package cloud.ashwin;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.table.api.Table;

public class KafkaJsonProcessingJob2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String inputTopic = "input_topic";
        String outputTopic = "output_topic";
        String kafkaBootstrapServers = "localhost:9092";

        System.out.println("******* Test printout");

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

//        kafkaInputStream.print();
//
        DataStream<String> testStream = kafkaInputStream.map(line -> {
            System.out.println("line: " + line);
            return line;
        });

        // Configure Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(kafkaBootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
//                .setKafkaProducerConfig(properties)
                .build();

        // Write to Kafka
        testStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("Kafka JSON Processing Job");
    }
}