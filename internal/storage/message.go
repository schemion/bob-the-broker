package storage

import "time"

type Message struct {
	Topic     string
	Offset    int64
	Key       string
	Value     string
	Partition int
	CreatedAt time.Time
}
