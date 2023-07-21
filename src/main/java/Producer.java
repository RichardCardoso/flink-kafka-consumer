import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

public class Producer {

    public static KafkaSink<String> producer() {

        KafkaRecordSerializationSchema<String> serializationSchema = KafkaRecordSerializationSchema.<String>builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic("test")
                .build();
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setRecordSerializer(serializationSchema)
                .build();

        return sink;
    }
}
