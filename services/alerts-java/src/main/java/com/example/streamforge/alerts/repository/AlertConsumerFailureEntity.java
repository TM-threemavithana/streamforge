package com.example.streamforge.alerts.repository;

import org.hibernate.annotations.CreationTimestamp;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import jakarta.persistence.Column;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Objects;

class AlertConsumerFailureId implements Serializable {
    private String consumerGroup;
    private String topic;
    private Integer partition;
    private Long offsetNum;

    public AlertConsumerFailureId() {}
    public AlertConsumerFailureId(String consumerGroup, String topic, Integer partition, Long offsetNum) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.partition = partition;
        this.offsetNum = offsetNum;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlertConsumerFailureId that = (AlertConsumerFailureId) o;
        return Objects.equals(consumerGroup, that.consumerGroup) &&
                Objects.equals(topic, that.topic) &&
                Objects.equals(partition, that.partition) &&
                Objects.equals(offsetNum, that.offsetNum);
    }
    @Override
    public int hashCode() {
        return Objects.hash(consumerGroup, topic, partition, offsetNum);
    }
}

@Entity
@Table(name = "alert_consumer_failures")
@IdClass(AlertConsumerFailureId.class)
public class AlertConsumerFailureEntity {

    @Id
    @Column(name = "consumer_group")
    private String consumerGroup;

    @Id
    @Column(name = "topic")
    private String topic;

    @Id
    @Column(name = "partition")
    private Integer partition;

    @Id
    @Column(name = "offset_num")
    private Long offsetNum;

    @Column(name = "message_key")
    private String messageKey;

    @Column(name = "payload_sha256")
    private String payloadSha256;

    @Column(name = "reason", nullable = false)
    private String reason;

    @CreationTimestamp
    @Column(name = "recorded_at", nullable = false, updatable = false)
    private ZonedDateTime recordedAt;

    // Getters and setters
    public String getConsumerGroup() { return consumerGroup; }
    public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public Integer getPartition() { return partition; }
    public void setPartition(Integer partition) { this.partition = partition; }
    public Long getOffsetNum() { return offsetNum; }
    public void setOffsetNum(Long offsetNum) { this.offsetNum = offsetNum; }
    public String getMessageKey() { return messageKey; }
    public void setMessageKey(String messageKey) { this.messageKey = messageKey; }
    public String getPayloadSha256() { return payloadSha256; }
    public void setPayloadSha256(String payloadSha256) { this.payloadSha256 = payloadSha256; }
    public String getReason() { return reason; }
    public void setReason(String reason) { this.reason = reason; }
    public ZonedDateTime getRecordedAt() { return recordedAt; }
    public void setRecordedAt(ZonedDateTime recordedAt) { this.recordedAt = recordedAt; }
}
