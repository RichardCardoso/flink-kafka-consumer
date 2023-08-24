package functions;

import misc.BroadcastStateDescriptor;
import models.LiveMessage;
import models.WindowElement;
import models.control.ControlMessage;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ControlBroadcastFunction extends BroadcastProcessFunction<LiveMessage, ControlMessage, WindowElement> {

    private final static Logger log = LoggerFactory.getLogger(ControlBroadcastFunction.class);

    @Override
    public void processElement(LiveMessage liveMessage, BroadcastProcessFunction<LiveMessage, ControlMessage, WindowElement>.ReadOnlyContext readOnlyContext, Collector<WindowElement> collector) throws Exception {

        ReadOnlyBroadcastState<String, ControlMessage> broadcastState = readOnlyContext.getBroadcastState(BroadcastStateDescriptor.getControlStateDescriptor());

        for (Map.Entry<String, ControlMessage> entry : broadcastState.immutableEntries()) {

            ControlMessage config = entry.getValue();
            if (config.getCustomerId() > 0) {
                log.info("Emmited filteredEvent, receivedTime => {}", liveMessage.getReceivedTime());
                collector.collect(new WindowElement(liveMessage, config));
            }
        }
    }

    @Override
    public void processBroadcastElement(ControlMessage controlMessage, BroadcastProcessFunction<LiveMessage, ControlMessage, WindowElement>.Context context, Collector<WindowElement> collector) throws Exception {

        String sKey = controlMessage.getCustomerId() + "_" + controlMessage.getAlertId();
        BroadcastState<String, ControlMessage> broadcastState = context.getBroadcastState(BroadcastStateDescriptor.getControlStateDescriptor());
        broadcastState.put(sKey, controlMessage);
    }
}