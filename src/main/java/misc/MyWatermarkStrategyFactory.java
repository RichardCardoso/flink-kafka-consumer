package misc;

import models.FilteredEvent;
import models.LiveMessage;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;

public class MyWatermarkStrategyFactory {

    private final static Logger log = LoggerFactory.getLogger(MyWatermarkStrategyFactory.class);

    public static WatermarkStrategy<FilteredEvent> filteredEventStrat() {

        return WatermarkStrategy
                .<FilteredEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> {
                    String sReceivedTime = event.getLiveMessage().getReceivedTime();
                    try {
                        Date date = Constants.DATE_FORMAT.parse(sReceivedTime);
                        return date.getTime();
                    } catch (ParseException ex) {
                        log.error("Failed to convert receivedTime {}", sReceivedTime, ex);
                        return Long.MIN_VALUE;
                    }
                });
    }

    public static WatermarkStrategy<LiveMessage> liveMessageStrat() {

        return WatermarkStrategy
                .<LiveMessage>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> {
                    String sReceivedTime = event.getReceivedTime();
                    try {
                        Date date = Constants.DATE_FORMAT.parse(sReceivedTime);
                        return date.getTime();
                    } catch (ParseException ex) {
                        log.error("Failed to convert receivedTime {}", sReceivedTime, ex);
                        return Long.MIN_VALUE;
                    }
                });
    }
}
