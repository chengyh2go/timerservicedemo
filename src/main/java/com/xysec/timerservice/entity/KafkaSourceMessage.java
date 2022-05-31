package com.xysec.timerservice.entity;

public class KafkaSourceMessage {
    public String value;
    public String topic;
    public long offset;
    public int partition;

    public KafkaSourceMessage() {
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "KafkaSourceMessage{" +
                "value='" + value + '\'' +
                ", topic='" + topic + '\'' +
                ", offset=" + offset +
                ", partition=" + partition +
                '}';
    }
}
