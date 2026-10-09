package com.example.streamforge.alerts.repository;

import java.io.Serializable;
import java.util.Objects;

public class AlertRuleId implements Serializable {
    private String ruleId;
    private Integer ruleVersion;

    public AlertRuleId() {}

    public AlertRuleId(String ruleId, Integer ruleVersion) {
        this.ruleId = ruleId;
        this.ruleVersion = ruleVersion;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public Integer getRuleVersion() {
        return ruleVersion;
    }

    public void setRuleVersion(Integer ruleVersion) {
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
