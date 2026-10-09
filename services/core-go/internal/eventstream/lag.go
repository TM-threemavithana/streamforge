package eventstream

import (
	"context"
	"fmt"
	"time"

	"github.com/example/streamforge/services/core-go/internal/domain"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type groupLagClient interface {
	Lag(context.Context, ...string) (kadm.DescribedGroupLags, error)
}

type LagReader struct {
	client groupLagClient
	topic  string
	group  string
	now    func() time.Time
}

func NewLagReader(client *kgo.Client, topic, group string) *LagReader {
	return &LagReader{client: kadm.NewClient(client), topic: topic, group: group, now: time.Now}
}

func (reader *LagReader) ReadConsumerLag(ctx context.Context) (domain.ConsumerLag, error) {
	groups, err := reader.client.Lag(ctx, reader.group)
	if err != nil {
		return domain.ConsumerLag{}, fmt.Errorf("read Kafka consumer lag: %w", err)
	}
	group, ok := groups[reader.group]
	if !ok {
		return domain.ConsumerLag{}, fmt.Errorf("Kafka consumer group %q was not returned", reader.group)
	}
	if err = group.Error(); err != nil {
		return domain.ConsumerLag{}, fmt.Errorf("read Kafka consumer group %q: %w", reader.group, err)
	}

	status := domain.ConsumerLag{
		Group:      reader.group,
		State:      group.State,
		Members:    len(group.Members),
		Partitions: make([]domain.ConsumerPartitionLag, 0, len(group.Lag)),
		ObservedAt: reader.now().UTC(),
	}
	for _, partition := range group.Lag.Sorted() {
		if partition.Topic != reader.topic {
			continue
		}
		if partition.Err != nil || partition.Lag < 0 {
			return domain.ConsumerLag{}, fmt.Errorf("read Kafka lag for %s partition %d: %v", partition.Topic, partition.Partition, partition.Err)
		}
		status.TotalLag += partition.Lag
		status.Partitions = append(status.Partitions, domain.ConsumerPartitionLag{
			Topic:           partition.Topic,
			Partition:       partition.Partition,
			CommittedOffset: partition.Commit.At,
			EndOffset:       partition.End.Offset,
			Lag:             partition.Lag,
		})
	}
	if len(status.Partitions) == 0 {
		return domain.ConsumerLag{}, fmt.Errorf("Kafka consumer group %q has no offsets for topic %q", reader.group, reader.topic)
	}
	return status, nil
}
