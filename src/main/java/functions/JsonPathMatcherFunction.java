package functions;

import com.jayway.jsonpath.JsonPath;
import misc.Constants;
import models.LiveMessage;
import models.NotificationCandidate;
import models.WindowElement;
import models.control.ComparisonRule;
import models.control.ControlMessage;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class JsonPathMatcherFunction extends ProcessAllWindowFunction<WindowElement, NotificationCandidate, TimeWindow> {

    private final static Logger log = LoggerFactory.getLogger(JsonPathMatcherFunction.class);

    @Override
    public void process(Context context, Iterable<WindowElement> elements, Collector<NotificationCandidate> collector) throws Exception {

        ControlMessage control = null;
        List<LiveMessage> liveMessages = new ArrayList<>();
        List<String> values = new ArrayList<>();
        Double avg;
        double sum = 0, max = Double.MIN_VALUE, min = Double.MAX_VALUE;
        int count = 0;
        boolean matches = true;
        ObjectMapper mapper = new ObjectMapper();

        for (WindowElement element : elements) {

            String json = mapper.writeValueAsString(element.getLiveMessage());
            Map<String, Object> ctx = JsonPath.parse(json).read("$");
            if (control == null) {
                control = element.getControlMessage(); // Assuming all elements in the window have the same control message
            }
            String sValue = JsonPath.parse(ctx).read(control.getTargetField(), String.class);
            double dValue = Double.parseDouble(sValue);
            sum += dValue;
            count++;
            max = Math.max(max, dValue);
            min = Math.min(min, dValue);
            liveMessages.add(element.getLiveMessage());
            values.add(element.getLiveMessage().getReceivedTime() + "|" + element.getLiveMessage().getValue());
        }
        avg = sum / count;

        if (Objects.isNull(control)) {
            log.error("No control message found in window elements.");
            return;
        }

        for (ComparisonRule rule : control.getComparison().getComparisonRules()) {
            double value;
            switch (control.getComparison().getRange().getAggregationType()) {
                case AVERAGE:
                    value = avg;
                    break;
                case MAX:
                    value = max;
                    break;
                case MIN:
                    value = min;
                    break;
                default:
                    log.warn("Skipping rule {}, invalid aggregation tyepe {}", rule, control.getComparison().getRange().getAggregationType());
                    continue;
            }
            matches &= rule.matches(value);
            log.info("Matching result for rule {}, reference {} => {}", rule, value, matches);
        }

        long start, end, maxTimeStamp;
        start = context.window().getStart();
        end = context.window().getEnd();
        maxTimeStamp = context.window().maxTimestamp();

        String sStart, sEnd, sMaxTimeStamp;
        sStart = Constants.DATE_FORMAT.format(new Date(start));
        sEnd = Constants.DATE_FORMAT.format(new Date(end));
        sMaxTimeStamp = Constants.DATE_FORMAT.format(new Date(maxTimeStamp));

        String sDebug =  String.format(
                " => window start: %s, window end: %s, window max late: %s, values: %s",
                sStart, sEnd, sMaxTimeStamp, values
        );

        if (matches) {
            sDebug = "JsonPath (MATCH FOUND)" + sDebug;
            collector.collect(new NotificationCandidate(liveMessages, control, sStart, sEnd));
        } else {
            sDebug = "JsonPath (NO MATCHES)" + sDebug;
        }
        log.info(sDebug);
    }


}
