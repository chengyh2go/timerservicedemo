package com.xysec.timerservice.deserialization;

import com.xysec.timerservice.entity.KafkaSourceMessage;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

public class DeserializationSchema implements KeyedDeserializationSchema<KafkaSourceMessage> {
    @Override
    public KafkaSourceMessage deserialize(byte[] key, byte[] message, String topic, int partition, long offset) throws IOException {
        KafkaSourceMessage kafkaSourceMessage = new KafkaSourceMessage();
        kafkaSourceMessage.value = new String(message, StandardCharsets.UTF_8);
        kafkaSourceMessage.topic = topic;
        kafkaSourceMessage.partition = partition;
        kafkaSourceMessage.offset = offset;
        return kafkaSourceMessage;
    }

    @Override
    public boolean isEndOfStream(KafkaSourceMessage kafkaSourceMessage) {
        return false;
    }

    @Override
    public TypeInformation<KafkaSourceMessage> getProducedType() {
        return getForClass(KafkaSourceMessage.class);
    }
}
