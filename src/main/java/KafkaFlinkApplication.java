import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class KafkaFlinkApplication {

    public static void main(String[] args) throws Exception {

        // Configure execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.fromSource(Consumer.consumer(), WatermarkStrategy.noWatermarks(), "Kafka Source");
        lines.print();

        System.out.println("Starting flink job");

        env.execute();
    }
}
