import models.FilteredEvent;
import models.LiveMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

public class Consumer<T, R extends DeserializationSchema<T>> {

    public KafkaSource<T> consumer(Map<String, String> params, String topic, String groupId, R schema, OffsetsInitializer offsetsInitializer) {

        String host = "kafka";
        String port = "29092";

        if (!StringUtils.isEmpty(params.get("host"))) {
            host = params.get("host");
        }
        if (!StringUtils.isEmpty(params.get("port"))) {
            port = params.get("port");
        }

        return KafkaSource.<T>builder()
                .setBootstrapServers(host + ":" + port)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(schema)
                .build();
    }
}
