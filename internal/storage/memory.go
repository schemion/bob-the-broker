package storage

import (
	"os"
	"strings"
	"sync"
	"time"
)

type Storage interface {
	AppendMessage(msg Message) (int64, error)
	FetchMessages(offset int64, limit int) ([]Message, error)
}

type memoryStorage struct {
	messages    []Message
	baseOffset  int64
	maxMessages int
	retention   time.Duration
	mu          sync.RWMutex
}

func NewMemoryStorage(maxMessages int) *memoryStorage {
	if maxMessages <= 0 {
		maxMessages = 10000
	}
	retention := parseDurationEnv("MESSAGE_TTL")
	return &memoryStorage{
		messages:    make([]Message, 0, maxMessages),
		baseOffset:  0,
		maxMessages: maxMessages,
		retention:   retention,
	}
}

func (m *memoryStorage) AppendMessage(msg Message) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	m.purgeExpiredLocked(now)

	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = now
	}
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
	// Opportunistic cleanup on reads as well.
	if m.retention > 0 {
		m.mu.Lock()
		m.purgeExpiredLocked(time.Now())
		m.mu.Unlock()
	}

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

func (m *memoryStorage) purgeExpiredLocked(now time.Time) {
	if m.retention <= 0 || len(m.messages) == 0 {
		return
	}
	cutoff := now.Add(-m.retention)
	drop := 0
	for drop < len(m.messages) && m.messages[drop].CreatedAt.Before(cutoff) {
		drop++
	}
	if drop == 0 {
		return
	}
	m.messages = m.messages[drop:]
	m.baseOffset += int64(drop)
}

func parseDurationEnv(name string) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return 0
	}
	return d
}
