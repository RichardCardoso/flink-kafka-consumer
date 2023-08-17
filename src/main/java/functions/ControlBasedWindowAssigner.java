package functions;

import models.ControlMessage;
import models.WindowElement;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public class JsonPathMatcherFunction extends WindowAssigner<Object, TimeWindow> {

    private final static Logger log = LoggerFactory.getLogger(JsonPathMatcherFunction.class);

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext windowAssignerContext) {

        WindowElement windowElement = (WindowElement) element;
        ControlMessage controlMessage = windowElement.getControlMessage();
        SlidingEventTimeWindows slidingEventTimeWindows = SlidingEventTimeWindows.of(
                Time.seconds(controlMessage.getWindowSize()),
                Time.seconds(controlMessage.getSlideSize())
        );
        return slidingEventTimeWindows.assignWindows(element, timestamp, windowAssignerContext);
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {

        return EventTimeTrigger.create();
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
