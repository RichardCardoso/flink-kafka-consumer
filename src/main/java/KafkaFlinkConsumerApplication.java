import functions.ControlBasedWindowAssigner;
import functions.ControlBroadcastFunction;
import functions.FullWindowResultsProcessor;
import functions.JsonPathMatcherFunction;
import misc.BroadcastStateDescriptor;
import misc.MyWatermarkStrategyFactory;
import models.LiveMessage;
import models.NotificationCandidate;
import models.WindowElement;
import models.control.ControlMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackendFactory;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import serialization.ControlMessageSchema;
import serialization.LiveMessageSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static misc.Constants.ALLOWED_LATENESS_IN_SECS;

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

        env.setStateBackend(new EmbeddedRocksDBStateBackend());

        env.enableCheckpointing(15000);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.setCheckpointTimeout(60000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.enableUnalignedCheckpoints();
        checkpointConfig.setCheckpointStorage("file:///var/flink/checkpoints");

        // DataSource
        DataStream<LiveMessage> liveMessages = env
                .fromSource(
                        new Consumer<LiveMessage, LiveMessageSchema>().consumer(params, "live", "flink-test", new LiveMessageSchema(), OffsetsInitializer.latest()),
                        WatermarkStrategy.noWatermarks(),
                        "KafkaLiveDataSource"
                ).name("LiveDataSource");

        // ControlSource
        DataStream<ControlMessage> controlMessages = env
                .fromSource(
                        new Consumer<ControlMessage, ControlMessageSchema>().consumer(params, "control", "flink-test", new ControlMessageSchema(), OffsetsInitializer.latest()),
                        WatermarkStrategy.noWatermarks(),
                        "KafkaControlSource"
                )
                .filter(Objects::nonNull)
                .filter(c -> Objects.nonNull(c.getCustomerId()) && c.getCustomerId() > 0L)
                .name("ControlSource");

        controlMessages.print();

        // Broadcasts control rules
        BroadcastStream<ControlMessage> broadcast = controlMessages.broadcast(BroadcastStateDescriptor.getControlStateDescriptor());

        KeyedStream<WindowElement, Object> controlAwareMessages = liveMessages
                .connect(broadcast)
                .process(new ControlBroadcastFunction())
                .assignTimestampsAndWatermarks(MyWatermarkStrategyFactory.filteredEventStrat())
                .name("ControlBroadcastFunction")
                .keyBy(event -> event.getControlMessage().getCustomerId() + "_" + event.getControlMessage().getAlertId() + "_" + event.getLiveMessage().getWellId());

        // Creates sliding windows based on control messages
        DataStream<NotificationCandidate> jsonPathMatchResult = controlAwareMessages
                .windowAll(new ControlBasedWindowAssigner())
//                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .allowedLateness(Time.seconds(ALLOWED_LATENESS_IN_SECS))
                .process(new JsonPathMatcherFunction())
                .name("JsonPathMatcherFunction");

        SingleOutputStreamOperator<NotificationCandidate> fullWindowResults = jsonPathMatchResult
                .process(new FullWindowResultsProcessor())
                .name("FullWindowResults");

        fullWindowResults.print();

        log.info("Starting flink job");

        env.execute();
    }
}
