package com.example.streamforge.alerts.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface AlertRuleRepository extends JpaRepository<AlertRuleEntity, AlertRuleId> {
    List<AlertRuleEntity> findByEnabledTrue();
    List<AlertRuleEntity> findAllByOrderByRuleIdAscRuleVersionDesc();
    List<AlertRuleEntity> findByRuleIdOrderByRuleVersionDesc(String ruleId);
    Optional<AlertRuleEntity> findTopByRuleIdOrderByRuleVersionDesc(String ruleId);
    Optional<AlertRuleEntity> findByRuleIdAndRuleVersion(String ruleId, Integer ruleVersion);
    boolean existsByRuleIdAndRuleVersion(String ruleId, Integer ruleVersion);
}
