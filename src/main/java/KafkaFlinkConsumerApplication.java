import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class KafkaFlinkConsumerApplication {

    public static void main(String[] args) throws Exception {

        Map<String, String> params = new HashMap<>();

        Stream.of(args)
                .filter(x -> !StringUtils.isEmpty(x))
                .filter(x -> x.indexOf(":") > 0 && x.indexOf(":") < x.length() - 1)
                .map(x -> x.split(":"))
                .forEach(x -> params.put(x[0], x[1]));

        System.out.println("Params: " + params);

        // Configure execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // DataSource
        DataStream<String> numbers = env.fromSource(
                Consumer.consumer(params, "test", "flink-test"),
                WatermarkStrategy.noWatermarks(),
                "KafkaSource");

        // ControlSource
//        DataStream<ControlMessage> controlSource = env.fromSource(
//                Consumer.consumer(params, "control", "flink-test"),
//                WatermarkStrategy.noWatermarks(),
//                "ControlSource");

        DataStream<String> windowedStream = numbers
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
                .process(new Consumer.MyProcessAllWindowFunction())
                .name("SlidingWindow");

        windowedStream.print();

        System.out.println("Starting flink job");

        env.execute();
    }
}
