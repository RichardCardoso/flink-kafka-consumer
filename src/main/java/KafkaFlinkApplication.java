import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class KafkaFlinkApplication {

    public static void main(String[] args) throws Exception {

        Map<String, String> params = new HashMap<>();

        Stream.of(args)
                .filter(x -> !StringUtils.isEmpty(x))
                .filter(x -> x.indexOf(":") > 0 && x.indexOf(":") < x.length() - 1)
                .map(x -> x.split(":"))
                .forEach(x -> params.put(x[0], x[1]));
        ;

        System.out.println("Params: " + params);

        // Configure execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.fromSource(Consumer.consumer(params), WatermarkStrategy.noWatermarks(), "Kafka Source");
        lines.print();

        System.out.println("Starting flink job");

        env.execute();
    }
}
