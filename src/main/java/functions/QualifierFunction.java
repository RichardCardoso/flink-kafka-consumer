package functions;

import models.WindowElement;
import models.QualifiedEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class QualifierFunction implements FlatMapFunction<WindowElement, QualifiedEvent> {
    @Override
    public void flatMap(WindowElement value, Collector<QualifiedEvent> out) throws Exception {
        try {

            // Should only collect if verified, using JsonPath, that the message matches the configured alert
            out.collect(new QualifiedEvent(value.getLiveMessage(), value.getControlMessage()));

            /*
            Map<String, Object> ctx = JsonPath.parse(value.getEvent().getPayload()).read("$");
            List<ControlMessage> controls = value.getControlMessageList();

            for (ControlMessage control : controls) {
                try {
                    String result = JsonPath.parse(ctx).read(control.getJsonPath(), String.class);

                    if (!result.isEmpty()) {
                        out.collect(new QualifiedEvent(value.getEvent(), control));
                    }
                } catch (Exception ignored) {
                    // Handle parsing exceptions if needed
                }
            }
            */
        } catch (Exception ignored) {
            // Handle parsing exceptions if needed
        }
    }
}
