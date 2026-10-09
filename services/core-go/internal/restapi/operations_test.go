package restapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
)

type lagReaderStub struct {
	lag domain.ConsumerLag
	err error
}

func (stub lagReaderStub) ReadConsumerLag(context.Context) (domain.ConsumerLag, error) {
	return stub.lag, stub.err
}

func TestKafkaLagReportsDirectDatabaseMode(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/operations/kafka-lag", nil)
	recorder := httptest.NewRecorder()
	(&Server{}).kafkaLag(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d", recorder.Code)
	}
	var response struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil || response.Enabled {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func TestKafkaLagReportsBrokerOffsets(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/operations/kafka-lag", nil)
	recorder := httptest.NewRecorder()
	server := &Server{lagReader: lagReaderStub{lag: domain.ConsumerLag{Group: "analytics", TotalLag: 4, ObservedAt: time.Now()}}}
	server.kafkaLag(recorder, request)
	if recorder.Code != http.StatusOK || !containsJSONNumber(recorder.Body.Bytes(), "total_lag", 4) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestKafkaLagSanitizesDependencyFailure(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/v1/operations/kafka-lag", nil)
	recorder := httptest.NewRecorder()
	server := &Server{lagReader: lagReaderStub{err: errors.New("secret broker detail")}}
	server.kafkaLag(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable || containsText(recorder.Body.Bytes(), "secret") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func containsJSONNumber(body []byte, field string, expected float64) bool {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return false
	}
	consumer, ok := value["consumer"].(map[string]any)
	return ok && consumer[field] == expected
}

func containsText(body []byte, value string) bool {
	for index := 0; index+len(value) <= len(body); index++ {
		if string(body[index:index+len(value)]) == value {
			return true
		}
	}
	return false
}
