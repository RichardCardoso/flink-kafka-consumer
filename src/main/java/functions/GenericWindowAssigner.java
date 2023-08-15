package functions;

import misc.Constants;
import misc.MyCustomTrigger;
import models.ControlMessage;
import models.FilteredEvent;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;

public class GenericWindowAssigner extends WindowAssigner<Object, TimeWindow> {

    private final static Logger log = LoggerFactory.getLogger(GenericWindowAssigner.class);

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext windowAssignerContext) {

        FilteredEvent filteredEvent = (FilteredEvent) element;
        ControlMessage controlMessage = filteredEvent.getControlMessage();
        long slidingWindowSizeSeconds = controlMessage.getWindowSize();
        long slidingWindowIntervalSeconds = controlMessage.getSlideSize();

        List<TimeWindow> windows = new ArrayList<>();
        long currentWindowStart = timestamp - (timestamp % (slidingWindowSizeSeconds * 1000));

        while (currentWindowStart <= timestamp) {
            long windowEnd = currentWindowStart + (slidingWindowSizeSeconds * 1000);
            TimeWindow window = new TimeWindow(currentWindowStart, windowEnd);
            windows.add(window);
            currentWindowStart += (slidingWindowIntervalSeconds * 1000);

            log.info(
                    "Created window for receivedTime {} with start {} and end {}",
                    Constants.DATE_FORMAT.format(new Date(timestamp)),
                    Constants.DATE_FORMAT.format(new Date(currentWindowStart)),
                    Constants.DATE_FORMAT.format(new Date(windowEnd))
            );
        }

        return windows;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {

//        return EventTimeTrigger.create();
        return new MyCustomTrigger<>(30L);
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {

        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {

        return true;
    }
}
