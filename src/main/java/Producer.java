import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;

import java.util.Map;

public class Producer {

    public static KafkaSink<String> producer(Map<String, String> params) {

        String host = "kafka";
        String port = "29092";

        if (!StringUtils.isEmpty(params.get("host"))) {
            host = params.get("host");
        }
        if (!StringUtils.isEmpty(params.get("port"))) {
            port = params.get("port");
        }

        KafkaRecordSerializationSchema<String> serializationSchema = KafkaRecordSerializationSchema.<String>builder()
                .setValueSerializationSchema(new SimpleStringSchema())
                .setTopic("test")
                .build();
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(host + ":" + port)
                .setRecordSerializer(serializationSchema)
                .build();

        return sink;
    }
}
