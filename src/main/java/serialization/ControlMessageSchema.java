package serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import models.ControlMessage;

import java.io.IOException;

public class ControlMessageSchema implements DeserializationSchema<ControlMessage> {

    @Override
    public ControlMessage deserialize(byte[] bytes) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(bytes, ControlMessage.class);
    }

    @Override
    public boolean isEndOfStream(ControlMessage controlMessage) {

        return false;
    }

    @Override
    public TypeInformation<ControlMessage> getProducedType() {

        return TypeExtractor.getForClass(ControlMessage.class);
    }
}
