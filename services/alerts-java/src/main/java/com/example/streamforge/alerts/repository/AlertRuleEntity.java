package com.example.streamforge.alerts.repository;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import jakarta.persistence.Column;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Objects;

class AlertRuleId implements Serializable {
    private String ruleId;
    private Integer ruleVersion;
    
    // getters, setters, equals, hashcode
    public AlertRuleId() {}
    public AlertRuleId(String ruleId, Integer ruleVersion) {
        this.ruleId = ruleId;
        this.ruleVersion = ruleVersion;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlertRuleId that = (AlertRuleId) o;
        return Objects.equals(ruleId, that.ruleId) && Objects.equals(ruleVersion, that.ruleVersion);
    }
    @Override
    public int hashCode() {
        return Objects.hash(ruleId, ruleVersion);
    }
}

@Entity
@Table(name = "alert_rules")
@IdClass(AlertRuleId.class)
public class AlertRuleEntity {

    @Id
    @Column(name = "rule_id")
    private String ruleId;

    @Id
    @Column(name = "rule_version")
    private Integer ruleVersion;

    @Column(name = "kind", nullable = false)
    private String kind;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "parameters", nullable = false)
    private Map<String, Object> parameters;

    @Column(name = "enabled", nullable = false)
    private boolean enabled;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private ZonedDateTime createdAt;

    // Getters and setters
    public String getRuleId() { return ruleId; }
    public void setRuleId(String ruleId) { this.ruleId = ruleId; }
    public Integer getRuleVersion() { return ruleVersion; }
    public void setRuleVersion(Integer ruleVersion) { this.ruleVersion = ruleVersion; }
    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public Map<String, Object> getParameters() { return parameters; }
    public void setParameters(Map<String, Object> parameters) { this.parameters = parameters; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public ZonedDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(ZonedDateTime createdAt) { this.createdAt = createdAt; }
}
