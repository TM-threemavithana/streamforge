package com.example.streamforge.alerts.repository;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AlertRepository extends JpaRepository<AlertEntity, String> {
    Page<AlertEntity> findByDatasetId(String datasetId, Pageable pageable);
    Page<AlertEntity> findByRuleId(String ruleId, Pageable pageable);
    Page<AlertEntity> findByDatasetIdAndRuleId(String datasetId, String ruleId, Pageable pageable);
}
