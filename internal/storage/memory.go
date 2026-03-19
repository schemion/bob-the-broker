package storage

import "sync"

type Storage interface {
	AppendMessage(msg Message) (int64, error)
	FetchMessages(offset int64, limit int) ([]Message, error)
}

type memoryStorage struct {
	messages []Message
	mu       sync.RWMutex
}

func NewMemoryStorage() *memoryStorage {
	return &memoryStorage{messages: make([]Message, 0)}
}

func (m *memoryStorage) AppendMessage(msg Message) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg.Offset = int64(len(m.messages))
	m.messages = append(m.messages, msg)
	return msg.Offset, nil
}

func (m *memoryStorage) FetchMessages(offset int64, limit int) ([]Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 || offset >= int64(len(m.messages)) {
		return []Message{}, nil
	}

	if offset < 0 {
		offset = 0
	}

	start := int(offset)
	end := min(start+limit, len(m.messages))

	out := make([]Message, end-start)
	copy(out, m.messages[start:end])
	return out, nil
}
