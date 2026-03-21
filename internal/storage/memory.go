package storage

import "sync"

type Storage interface {
	AppendMessage(msg Message) (int64, error)
	FetchMessages(offset int64, limit int) ([]Message, error)
}

type memoryStorage struct {
	messages   []Message
	baseOffset int64
	maxMessages int
	mu         sync.RWMutex
}

func NewMemoryStorage(maxMessages int) *memoryStorage {
	if maxMessages <= 0 {
		maxMessages = 10000
	}
	return &memoryStorage{
		messages:    make([]Message, 0, maxMessages),
		baseOffset:  0,
		maxMessages: maxMessages,
	}
}

func (m *memoryStorage) AppendMessage(msg Message) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg.Offset = m.baseOffset + int64(len(m.messages))
	m.messages = append(m.messages, msg)

	if len(m.messages) > m.maxMessages {
		drop := len(m.messages) - m.maxMessages
		m.messages = m.messages[drop:]
		m.baseOffset += int64(drop)
	}

	return msg.Offset, nil
}

func (m *memoryStorage) FetchMessages(offset int64, limit int) ([]Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if limit <= 0 {
		return []Message{}, nil
	}

	if offset < m.baseOffset {
		offset = m.baseOffset
	}

	maxOffset := m.baseOffset + int64(len(m.messages))
	if offset >= maxOffset {
		return []Message{}, nil
	}

	start := int(offset - m.baseOffset)
	end := min(start+limit, len(m.messages))

	out := make([]Message, end-start)
	copy(out, m.messages[start:end])
	return out, nil
}
