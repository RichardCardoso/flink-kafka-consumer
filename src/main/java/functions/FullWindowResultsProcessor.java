package functions;

import models.NotificationCandidate;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class FullWindowResultsProcessor extends ProcessFunction<NotificationCandidate, NotificationCandidate> {

    private final static Logger log = LoggerFactory.getLogger(FullWindowResultsProcessor.class);

    @Override
    public void processElement(NotificationCandidate notificationCandidate, ProcessFunction<NotificationCandidate, NotificationCandidate>.Context context, Collector<NotificationCandidate> collector) throws Exception {

        String sDebug = String.format(
                " | values => %s, totalItems => %d, start => %s, end => %s",
                notificationCandidate.getLiveMessages(),
                notificationCandidate.getLiveMessages().size(),
                notificationCandidate.getStartTime(),
                notificationCandidate.getEndTime()
        );

        long lengthInSeconds = TimeUnit.MILLISECONDS.toSeconds(notificationCandidate.getControlMessage().getComparison().getRange().getLengthInMilliseconds());

        if (notificationCandidate.getLiveMessages().size() == lengthInSeconds) {
            log.info("Notification target" + sDebug);
            collector.collect(notificationCandidate);
        }
    }
}
