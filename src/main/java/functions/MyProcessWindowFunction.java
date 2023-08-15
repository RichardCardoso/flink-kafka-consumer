package functions;

import models.FilteredEvent;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MyProcessWindowFunction extends KeyedProcessFunction<String, FilteredEvent, FilteredEvent> {

    @Override
    public void processElement(FilteredEvent filteredEvent, KeyedProcessFunction<String, FilteredEvent, FilteredEvent>.Context context, Collector<FilteredEvent> collector) throws Exception {

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<String, FilteredEvent, FilteredEvent>.OnTimerContext ctx, Collector<FilteredEvent> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
    }
}
