package broker

import "bob-the-broker/internal/storage"

func NewMemoryStorage() storage.Storage {
	return storage.NewMemoryStorage()
}
