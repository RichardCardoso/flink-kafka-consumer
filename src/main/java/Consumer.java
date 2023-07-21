import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import java.util.Map;

public class Consumer {

    public static KafkaSource<String> consumer(Map<String, String> params) {

        String host = "kafka";
        String port = "29092";

        if (!StringUtils.isEmpty(params.get("host"))) {
            host = params.get("host");
        }
        if (!StringUtils.isEmpty(params.get("port"))) {
            port = params.get("port");
        }

        return KafkaSource.<String>builder()
                .setBootstrapServers(host + ":" + port)
                .setTopics("test")
                .setGroupId("flink-test")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
