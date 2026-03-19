package broker

import "bob-the-broker/internal/storage"

type Partition struct {
	storage storage.Storage
}

func (p *Partition) AppendMessage(msg storage.Message) (int64, error) {
	return p.storage.AppendMessage(msg)
}

func (p *Partition) FetchMessages(offset int64, limit int) ([]storage.Message, error) {
	return p.storage.FetchMessages(offset, limit)
}
