package broker

import (
	"errors"

	"bob-the-broker/internal/storage"
)

func (b *Broker) CreateTopic(name string, partitions int) error {
	if name == "" {
		return errors.New("topic name is required")
	}

	if b.topics == nil {
		b.topics = make(map[string]*Topic)
	}

	if _, exists := b.topics[name]; exists {
		return errors.New("topic already exists")
	}

	b.topics[name] = NewTopic(partitions, func() storage.Storage {
		return NewMemoryStorage()
	})
	return nil
}
