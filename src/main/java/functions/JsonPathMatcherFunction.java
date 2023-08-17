package functions;

import com.jayway.jsonpath.JsonPath;
import misc.Constants;
import misc.Operator;
import models.ControlMessage;
import models.LiveMessage;
import models.NotificationCandidate;
import models.WindowElement;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class MyProcessAllWindowFunction extends ProcessAllWindowFunction<WindowElement, NotificationCandidate, TimeWindow> {

    private final static Logger log = LoggerFactory.getLogger(MyProcessAllWindowFunction.class);

    @Override
    public void process(Context context, Iterable<WindowElement> elements, Collector<NotificationCandidate> collector) throws Exception {

        int total = 0, matching = 0;
        double matchingPercent = 0;
        ControlMessage control = null;
        List<LiveMessage> liveMessages = new ArrayList<>();
        List<String> values = new ArrayList<>();

        for (WindowElement element : elements) {
            total++;
            ObjectMapper mapper = new ObjectMapper();
            String json = mapper.writeValueAsString(element.getLiveMessage());
            Map<String, Object> ctx = JsonPath.parse(json).read("$");
            if (control == null) {
                control = element.getControlMessage(); // Assuming all elements in the window have the same control message
            }
            String sValue = JsonPath.parse(ctx).read(control.getTargetField(), String.class);
            Double dValue = Double.valueOf(sValue);
            if (Operator.matches(dValue, control.getValue().doubleValue(), control.getOperator())) {
                matching++;
//                values.add(element.getLiveMessage().getReceivedTime() + "|" + element.getLiveMessage().getValue());
            } else {
//                values.add("");
            }
            liveMessages.add(element.getLiveMessage());
            values.add(element.getLiveMessage().getReceivedTime() + "|" + element.getLiveMessage().getValue());
        }

        if (control != null) {
            matchingPercent = (double) matching / total;
            Long start, end, max;
            start = context.window().getStart();
            end = context.window().getEnd();
            max = context.window().maxTimestamp();
            String sStart, sEnd, sMax;
            sStart = Constants.DATE_FORMAT.format(new Date(start));
            sEnd = Constants.DATE_FORMAT.format(new Date(end));
            sMax = Constants.DATE_FORMAT.format(new Date(max));
            String sDebug =  String.format(
                    " => matchingPercent: %f, matching: %d, total: %d, window start: %s, window end: %s, window max late: %s, values: %s",
                    matchingPercent, matching, total, sStart, sEnd, sMax, values
            );
            if (matchingPercent >= control.getThresholdPercent()) {
//                sDebug = "value was over threshold during the window" + sDebug;
                collector.collect(new NotificationCandidate(liveMessages, control, matchingPercent));
            } else {
//                sDebug = "NO" + sDebug;
            }
//            log.info(sDebug);
        }
    }


}
