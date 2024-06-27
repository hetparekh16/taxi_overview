package Deserializer;

import Dto.Taxilocations;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class JSONValueDeserializationSchema implements DeserializationSchema <Taxilocations> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override

    public Taxilocations deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes , Taxilocations.class);
    }

    @Override
    public boolean isEndOfStream(Taxilocations taxilocations) {
        return false;
    }

    @Override
    public TypeInformation<Taxilocations> getProducedType() {
        return TypeInformation.of(Taxilocations.class);
    }
}
