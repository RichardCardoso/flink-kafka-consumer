package misc;

import models.control.ControlMessage;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class BroadcastStateDescriptor {

    public static MapStateDescriptor<String, ControlMessage> getControlStateDescriptor() {

        return new MapStateDescriptor<>(
                "globalControlMessagesState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(ControlMessage.class)
        );
    }
}
