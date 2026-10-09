package com.example.streamforge.alerts.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface AlertEventOutcomeRepository extends JpaRepository<AlertEventOutcomeEntity, AlertEventOutcomeId> {
}
