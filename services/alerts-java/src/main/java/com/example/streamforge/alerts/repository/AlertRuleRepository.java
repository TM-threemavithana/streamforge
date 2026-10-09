package com.example.streamforge.alerts.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface AlertRuleRepository extends JpaRepository<AlertRuleEntity, AlertRuleId> {
    List<AlertRuleEntity> findByEnabledTrue();
}
