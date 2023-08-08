import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Map;

public class Consumer {

    public static KafkaSource<String> consumer(Map<String, String> params, String topic, String groupId) {

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
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static class MyProcessAllWindowFunction extends ProcessAllWindowFunction<String, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<String> elements, Collector<String> collector) throws Exception {

            boolean isContinuousOverControlValue = true;
            int count = 0;
            for (String element : elements) {
                count++;
                int value = Integer.parseInt(element);
                if (value < 3000) {
                    isContinuousOverControlValue = false;
                    break;
                }
            }
            if (isContinuousOverControlValue && count >= 5) {
                collector.collect("value was over 3000 during the last 5 seconds");
            } else {
                collector.collect("no");
            }
        }
    }
}
