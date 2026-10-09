package domain

import "time"

type ConsumerPartitionLag struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	EndOffset       int64  `json:"end_offset"`
	Lag             int64  `json:"lag"`
}

type ConsumerLag struct {
	Group      string                 `json:"group"`
	State      string                 `json:"state"`
	Members    int                    `json:"members"`
	TotalLag   int64                  `json:"total_lag"`
	Partitions []ConsumerPartitionLag `json:"partitions"`
	ObservedAt time.Time              `json:"observed_at"`
}
