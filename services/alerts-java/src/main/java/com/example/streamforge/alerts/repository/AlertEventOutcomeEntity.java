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

class AlertEventOutcomeId implements Serializable {
    private String consumerGroup;
    private String topic;
    private Integer partition;
    private Long offsetNum;

    public AlertEventOutcomeId() {}
    public AlertEventOutcomeId(String consumerGroup, String topic, Integer partition, Long offsetNum) {
        this.consumerGroup = consumerGroup;
        this.topic = topic;
        this.partition = partition;
        this.offsetNum = offsetNum;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlertEventOutcomeId that = (AlertEventOutcomeId) o;
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
@Table(name = "alert_event_outcomes")
@IdClass(AlertEventOutcomeId.class)
public class AlertEventOutcomeEntity {

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

    @Column(name = "outcome", nullable = false)
    private String outcome;

    @CreationTimestamp
    @Column(name = "recorded_at", nullable = false, updatable = false)
    private ZonedDateTime recordedAt;

    public String getConsumerGroup() { return consumerGroup; }
    public void setConsumerGroup(String consumerGroup) { this.consumerGroup = consumerGroup; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public Integer getPartition() { return partition; }
    public void setPartition(Integer partition) { this.partition = partition; }
    public Long getOffsetNum() { return offsetNum; }
    public void setOffsetNum(Long offsetNum) { this.offsetNum = offsetNum; }
    public String getOutcome() { return outcome; }
    public void setOutcome(String outcome) { this.outcome = outcome; }
    public ZonedDateTime getRecordedAt() { return recordedAt; }
    public void setRecordedAt(ZonedDateTime recordedAt) { this.recordedAt = recordedAt; }
}
