package broker

import (
	"errors"

	"bob-the-broker/internal/storage"
)

type Broker struct {
	topics map[string]*Topic
}

func (b *Broker) Produce(topicName, key string, value []byte) error {
	if b.topics == nil {
		b.topics = make(map[string]*Topic)
	}

	topic := b.topics[topicName]
	if topic == nil {
		topic = NewTopic(1, func() storage.Storage {
			return NewMemoryStorage()
		})
		b.topics[topicName] = topic
	}

	p := topic.GetPartition(key)

	_, err := p.AppendMessage(Message{Key: key, Value: value})
	return err
}

func (b *Broker) Fetch(topicName string, partition int, offset int64, limit int) ([]Message, error) {
	if b.topics == nil {
		return nil, errors.New("topic not found")
	}

	topic := b.topics[topicName]
	if topic == nil {
		return nil, errors.New("topic not found")
	}

	if partition < 0 || partition >= len(topic.partitions) {
		return nil, errors.New("partition out of range")
	}

	return topic.partitions[partition].FetchMessages(offset, limit)
}
