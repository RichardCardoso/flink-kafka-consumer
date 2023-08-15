package serialization;

import models.ControlMessage;
import models.LiveMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class LiveMessageSchema implements DeserializationSchema<LiveMessage> {

    @Override
    public LiveMessage deserialize(byte[] bytes) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(bytes, LiveMessage.class);
    }

    @Override
    public boolean isEndOfStream(LiveMessage controlMessage) {

        return false;
    }

    @Override
    public TypeInformation<LiveMessage> getProducedType() {

        return TypeExtractor.getForClass(LiveMessage.class);
    }
}
