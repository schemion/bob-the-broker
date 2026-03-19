package broker

import (
	"bob-the-broker/internal/storage"
	"errors"
)

type Broker interface {
	Produce(topicName, key string, value string) error
	Fetch(topicName string, partition int, offset int64, limit int) ([]storage.Message, error)
	CreateTopic(name string, partitions int) error
}

type impl struct {
	topics map[string]*Topic
}

func NewBroker() *impl {
	return &impl{
		topics: make(map[string]*Topic),
	}
}

func (b *impl) Produce(topicName, key string, value string) error {
	topic, ok := b.topics[topicName]
	if !ok {
		topic = NewTopic(1, func() queue {
			return storage.NewMemoryStorage()
		})
		b.topics[topicName] = topic
	}

	p := topic.GetPartition(key)

	_, err := p.AppendMessage(storage.Message{Key: key, Value: value})
	return err
}

func (b *impl) Fetch(topicName string, partition int, offset int64, limit int) ([]storage.Message, error) {
	if b.topics == nil {
		return nil, errors.New("topic not found")
	}

	topic, ok := b.topics[topicName]
	if !ok {
		return nil, errors.New("topic not found")
	}

	if partition < 0 || partition >= len(topic.partitions) {
		return nil, errors.New("partition out of range")
	}

	return topic.partitions[partition].FetchMessages(offset, limit)
}
