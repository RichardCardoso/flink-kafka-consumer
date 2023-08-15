import functions.GenericWindowAssigner;
import functions.MyBroadcastProcessFunction;
import functions.MyProcessAllWindowFunction;
import misc.BroadcastStateDescriptor;
import misc.MyWatermarkStrategyFactory;
import models.LiveMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import models.ControlMessage;
import models.FilteredEvent;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serialization.ControlMessageSchema;
import serialization.LiveMessageSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class KafkaFlinkConsumerApplication {

    private final static Logger log = LoggerFactory.getLogger(KafkaFlinkConsumerApplication.class);

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
        env.enableCheckpointing(15000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointStorage("file:///tmp/flink/checkpoints");

        // DataSource
        DataStream<LiveMessage> numbers = env
                .fromSource(
                        new Consumer<LiveMessage, LiveMessageSchema>().consumer(params, "live", "flink-test", new LiveMessageSchema(), OffsetsInitializer.latest()),
                        WatermarkStrategy.noWatermarks(),
                        "KafkaSource"
                );

        // ControlSource
        DataStream<ControlMessage> controlSource = env
                .fromSource(
                        new Consumer<ControlMessage, ControlMessageSchema>().consumer(params, "control", "flink-test", new ControlMessageSchema(), OffsetsInitializer.earliest()),
                        WatermarkStrategy.noWatermarks(),
                        "KafkaControlSource"
                )
                .filter(Objects::nonNull)
                .filter(c -> Objects.nonNull(c.getCustomerId()))
                .name("ControlSource");

        controlSource.print();

        // Broadcasts global rules
        BroadcastStream<ControlMessage> broadcast = controlSource.broadcast(BroadcastStateDescriptor.getControlStateDescriptor());

        KeyedStream<FilteredEvent, Object> usersWithAccess = numbers
                .connect(broadcast)
                .process(new MyBroadcastProcessFunction())
                .assignTimestampsAndWatermarks(MyWatermarkStrategyFactory.filteredEventStrat())
                .name("Filtering function")
                .keyBy(event -> event.getControlMessage().getCustomerId() + "_" + event.getControlMessage().getAlertId());

        // Creates sliding windows based on control messages
        DataStream<String> userWindowsStream = usersWithAccess
                .windowAll(new GenericWindowAssigner())
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .allowedLateness(Time.seconds(10))
                .process(new MyProcessAllWindowFunction())
                .name("SlidingWindow");

//        userWindowsStream.print();

        log.info("Starting flink job");

        env.execute();
    }
}
