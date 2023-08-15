package functions;

import misc.BroadcastStateDescriptor;
import misc.Constants;
import models.ControlMessage;
import models.FilteredEvent;
import models.LiveMessage;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

public class MyBroadcastProcessFunction extends BroadcastProcessFunction<LiveMessage, ControlMessage, FilteredEvent> {

    private final static Logger log = LoggerFactory.getLogger(MyBroadcastProcessFunction.class);

    @Override
    public void processElement(LiveMessage liveMessage, BroadcastProcessFunction<LiveMessage, ControlMessage, FilteredEvent>.ReadOnlyContext readOnlyContext, Collector<FilteredEvent> collector) throws Exception {

        ReadOnlyBroadcastState<String, ControlMessage> broadcastState = readOnlyContext.getBroadcastState(BroadcastStateDescriptor.getControlStateDescriptor());



        for (Map.Entry<String, ControlMessage> entry : broadcastState.immutableEntries()) {

            ControlMessage config = entry.getValue();
            if (config.getCustomerId() > 0) {
                log.info("Emmited filteredEvent, receivedTime => {}", liveMessage.getReceivedTime());
                collector.collect(new FilteredEvent(liveMessage, config));
            }
        }
    }

    @Override
    public void processBroadcastElement(ControlMessage controlMessage, BroadcastProcessFunction<LiveMessage, ControlMessage, FilteredEvent>.Context context, Collector<FilteredEvent> collector) throws Exception {

        BroadcastState<String, ControlMessage> broadcastState = context.getBroadcastState(BroadcastStateDescriptor.getControlStateDescriptor());
        broadcastState.put(controlMessage.getCustomerId() + "_" + controlMessage.getAlertId(), controlMessage);
    }
}