package cloud.ashwin;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Properties;

public class MSKFlinkApp {
    public static void main(String[] args) throws Exception {
        // Set up the Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka properties for AWS IAM authentication
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name());
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "AWS_MSK_IAM");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                "software.amazon.msk.auth.iam.IAMLoginModule required;");
        properties.setProperty(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
                "software.amazon.msk.auth.iam.IAMClientCallbackHandler");

        // Configure Kafka source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("your-msk-bootstrap-servers")
                .setTopics("input-topic")
                .setGroupId("your-consumer-group-id")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(properties)
                .build();

        // Read from Kafka source
        DataStream<String> inputStream = env.fromSource(source, null, "Kafka Source");

        // Process the data (example: convert to uppercase)
        DataStream<String> processedStream = inputStream.map(String::toUpperCase);

        // Configure Kafka sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("your-msk-bootstrap-servers")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("output-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(properties)
                .build();

        // Write to Kafka sink
        processedStream.sinkTo(sink);

        // Execute the Flink job
        env.execute("MSK Flink App");
    }
}
