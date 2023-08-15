package functions;

import models.ControlMessage;
import models.FilteredEvent;
import models.LiveMessage;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FilterFunction extends RichCoFlatMapFunction<ControlMessage, LiveMessage, FilteredEvent> {

    static AtomicReference<List<ControlMessage>> configs = new AtomicReference<>(new ArrayList<>());

    @Override
    public void flatMap1(ControlMessage controlMessage, Collector<FilteredEvent> collector) throws Exception {

        configs.get().removeIf(x -> (x.getCustomerId().equals(controlMessage.getCustomerId()) && x.getAlertId().equals(controlMessage.getAlertId())));
        configs.get().add(controlMessage);
    }

    @Override
    public void flatMap2(LiveMessage s, Collector<FilteredEvent> collector) throws Exception {

        List<ControlMessage> controlMessages;

        // Must check if user has access to that message - String s will be replaced with java POJO for live data
        // For the moment, just using all messages received
        controlMessages = new ArrayList<>(configs.get());

        for (ControlMessage config : controlMessages) {
            collector.collect(new FilteredEvent(s, config));
        }

    }
}
