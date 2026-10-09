package eventstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

type groupLagClientStub struct {
	groups kadm.DescribedGroupLags
	err    error
}

func (stub groupLagClientStub) Lag(context.Context, ...string) (kadm.DescribedGroupLags, error) {
	return stub.groups, stub.err
}

func TestLagReaderUsesCommittedAndEndOffsetsForConfiguredTopic(t *testing.T) {
	observedAt := time.Date(2026, 10, 9, 1, 2, 3, 0, time.FixedZone("test", 3600))
	reader := &LagReader{
		client: groupLagClientStub{groups: kadm.DescribedGroupLags{
			"analytics": {
				Group: "analytics",
				State: "Stable",
				Lag: kadm.GroupLag{
					"raw": {
						0: {Topic: "raw", Partition: 0, Commit: kadm.Offset{At: 7}, End: kadm.ListedOffset{Offset: 10}, Lag: 3},
						1: {Topic: "raw", Partition: 1, Commit: kadm.Offset{At: 8}, End: kadm.ListedOffset{Offset: 8}, Lag: 0},
					},
					"other": {0: {Topic: "other", Partition: 0, Commit: kadm.Offset{At: 1}, End: kadm.ListedOffset{Offset: 9}, Lag: 8}},
				},
			},
		}},
		topic: "raw",
		group: "analytics",
		now:   func() time.Time { return observedAt },
	}

	got, err := reader.ReadConsumerLag(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.TotalLag != 3 || len(got.Partitions) != 2 || got.Partitions[0].CommittedOffset != 7 || got.Partitions[0].EndOffset != 10 {
		t.Fatalf("unexpected lag: %+v", got)
	}
	if got.ObservedAt.Location() != time.UTC {
		t.Fatalf("observed_at is not UTC: %v", got.ObservedAt)
	}
}

func TestLagReaderPropagatesBrokerFailure(t *testing.T) {
	reader := &LagReader{client: groupLagClientStub{err: errors.New("broker unavailable")}, topic: "raw", group: "analytics", now: time.Now}
	if _, err := reader.ReadConsumerLag(context.Background()); err == nil {
		t.Fatal("expected broker failure")
	}
}

func TestLagReaderDoesNotReportZeroForGroupWithoutTopicOffsets(t *testing.T) {
	reader := &LagReader{
		client: groupLagClientStub{groups: kadm.DescribedGroupLags{"analytics": {Group: "analytics", State: "Dead"}}},
		topic:  "raw", group: "analytics", now: time.Now,
	}
	if _, err := reader.ReadConsumerLag(context.Background()); err == nil {
		t.Fatal("expected missing topic offsets to be unavailable")
	}
}
