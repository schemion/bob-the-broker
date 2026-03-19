package broker

import (
	"bob-the-broker/internal/storage"
	"hash/fnv"
)

type queue interface {
	AppendMessage(msg storage.Message) (int64, error)
	FetchMessages(offset int64, limit int) ([]storage.Message, error)
}

type Topic struct {
	partitions []*Partition
}

func NewTopic(partitionCount int, storageFactory func() queue) *Topic {
	if partitionCount <= 0 {
		partitionCount = 1
	}

	partitions := make([]*Partition, 0, partitionCount)
	for i := 0; i < partitionCount; i++ {
		partitions = append(partitions, &Partition{storage: storageFactory()})
	}

	return &Topic{partitions: partitions}
}

func (t *Topic) GetPartition(key string) *Partition {
	partitionIndex := hash(key) % len(t.partitions)
	return t.partitions[partitionIndex]
}

func hash(key string) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32())
}
