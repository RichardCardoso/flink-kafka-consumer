package misc;

import models.ControlMessage;
import models.FilteredEvent;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class MyCustomTrigger<W extends Window> extends Trigger<Object, W> {

    private final static Logger log = LoggerFactory.getLogger(MyCustomTrigger.class);

    private boolean fired = false;
    private final long minIntervalMillis;

    public MyCustomTrigger(long minIntervalSeconds) {
        this.minIntervalMillis = minIntervalSeconds * 1000;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {

        // Trigger when the watermark passes the end of the allowed lateness
        if (ctx.getCurrentWatermark() >= window.maxTimestamp()) {
            log.info("Checking if watermark was fired, current {}, maxTimeStamp {}", ctx.getCurrentWatermark(), window.maxTimestamp());
            if (!fired) {
                log.info("Entering watermark fine routine, current {}, maxTimeStamp {}", ctx.getCurrentWatermark(), window.maxTimestamp());
                FilteredEvent filteredEvent = (FilteredEvent) element;
                ControlMessage controlMessage = filteredEvent.getControlMessage();

                fired = true;
                log.info("Window {} fired and purged", window);
                return TriggerResult.FIRE;
            }
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {

        if (time >= (window.maxTimestamp()) && !fired) {
            log.info(
                    "window {} => onEventTime FIRE_AND_PURGE => Time {}, maxTimeStamp {}",
                    window, Constants.DATE_FORMAT.format(new Date(time)), Constants.DATE_FORMAT.format(new Date(window.maxTimestamp()))
            );
            fired = true;
            return TriggerResult.FIRE;
        } else {
            log.info(
                    "window {} => onEventTime CONTINUE =>Time {}, maxTimeStamp {}",
                    window, Constants.DATE_FORMAT.format(new Date(time)), Constants.DATE_FORMAT.format(new Date(window.maxTimestamp()))
            );
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {

        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {

        fired = false;
    }
}
