package broker

import (
	"errors"

	"bob-the-broker/internal/storage"
)

func (b *impl) CreateTopic(name string, partitions int) error {
	if name == "" {
		return errors.New("topic name is required")
	}

	if _, exists := b.topics[name]; exists {
		return errors.New("topic already exists")
	}

	b.topics[name] = NewTopic(partitions, func() queue {
		return storage.NewMemoryStorage()
	})
	return nil
}
