import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class Consumer {

    public static KafkaSource<String> consumer() {

        return KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")
                .setTopics("test")
                .setGroupId("flink-test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
