package eventstream

import (
	"context"
	"errors"
	"testing"

	"github.com/example/streamforge/services/core-go/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type processorStub struct {
	events     int
	rejections int
	err        error
}

func (processor *processorStub) ProcessEvent(context.Context, domain.TripEvent) (domain.EventResult, error) {
	processor.events++
	return domain.EventResult{}, processor.err
}

func (processor *processorStub) ReportSourceRejection(context.Context, domain.SourceRejection) (domain.EventResult, error) {
	processor.rejections++
	return domain.EventResult{}, processor.err
}

type failureRecorderStub struct {
	failures []ConsumerFailure
	err      error
}

func (recorder *failureRecorderStub) RecordConsumerFailure(_ context.Context, failure ConsumerFailure) error {
	recorder.failures = append(recorder.failures, failure)
	return recorder.err
}

func TestHandleRecordRoutesTripToDurableProcessor(t *testing.T) {
	processor := &processorStub{}
	failures := &failureRecorderStub{}
	consumer := NewConsumer(nil, "streamforge-analytics-v1", processor, failures)
	payload, err := EncodeTrip(domain.TripEvent{DatasetID: "dataset", RunID: "run", EventID: "event"})
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: "raw", Partition: 2, Offset: 7, Key: []byte("event"), Value: payload}
	if err = consumer.HandleRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if processor.events != 1 || len(failures.failures) != 0 {
		t.Fatalf("events=%d failures=%d", processor.events, len(failures.failures))
	}
}

func TestHandleRecordPersistsPermanentFailure(t *testing.T) {
	processor := &processorStub{err: domain.ErrInvalidInput}
	failures := &failureRecorderStub{}
	consumer := NewConsumer(nil, "streamforge-analytics-v1", processor, failures)
	payload, _ := EncodeTrip(domain.TripEvent{DatasetID: "dataset", RunID: "run", EventID: "event"})
	record := &kgo.Record{Topic: "raw", Partition: 1, Offset: 9, Key: []byte("event"), Value: payload}
	if err := consumer.HandleRecord(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if len(failures.failures) != 1 || failures.failures[0].Offset != 9 || len(failures.failures[0].PayloadSHA256) != 64 {
		t.Fatalf("unexpected failures: %+v", failures.failures)
	}
}

func TestHandleRecordLeavesTransientFailureUnresolved(t *testing.T) {
	processor := &processorStub{err: errors.New("database unavailable")}
	failures := &failureRecorderStub{}
	consumer := NewConsumer(nil, "streamforge-analytics-v1", processor, failures)
	payload, _ := EncodeTrip(domain.TripEvent{DatasetID: "dataset", RunID: "run", EventID: "event"})
	err := consumer.HandleRecord(context.Background(), &kgo.Record{Topic: "raw", Partition: 0, Offset: 3, Value: payload})
	if err == nil || len(failures.failures) != 0 {
		t.Fatalf("err=%v failures=%d", err, len(failures.failures))
	}
}
