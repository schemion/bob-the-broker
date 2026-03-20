package broker

import (
	"bob-the-broker/internal/storage"
	"errors"
	"log"
	"sync"
)

type Broker interface {
	Produce(topicName, key string, value string) error
	Fetch(topicName string, partition int, offset int64, limit int) ([]storage.Message, error)
	CreateTopic(name string, partitions int) error
	Subscribe(topicName string) chan storage.Message
	Unsubscribe(topicName string, ch chan storage.Message)
}

type impl struct {
	mu          sync.RWMutex
	topics      map[string]*Topic
	subscribers map[string]map[chan storage.Message]struct{}
}

func NewBroker() *impl {
	return &impl{
		topics:      make(map[string]*Topic),
		subscribers: make(map[string]map[chan storage.Message]struct{}),
	}
}

func (b *impl) Subscribe(topicName string) chan storage.Message {
	ch := make(chan storage.Message, 16)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.subscribers[topicName] == nil {
		b.subscribers[topicName] = make(map[chan storage.Message]struct{})
	}
	b.subscribers[topicName][ch] = struct{}{}
	return ch
}

func (b *impl) Unsubscribe(topicName string, ch chan storage.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()

	subs, ok := b.subscribers[topicName]
	if !ok {
		log.Printf("broker: unsubscribe ignored, topic not found: %s", topicName)
		return
	}

	delete(subs, ch)

	if len(subs) == 0 {
		delete(b.subscribers, topicName)
	}
}

func (b *impl) Produce(topicName, key string, value string) error {
	var subs []chan storage.Message

	b.mu.Lock()
	topic, ok := b.topics[topicName]
	if !ok {
		topic = NewTopic(1, func() queue {
			return storage.NewMemoryStorage()
		})
		b.topics[topicName] = topic
		log.Printf("broker: created topic %s with 1 partition", topicName)
	}
	for ch := range b.subscribers[topicName] {
		subs = append(subs, ch)
	}
	b.mu.Unlock()

	p := topic.GetPartition(key)

	msg := storage.Message{
		Topic: topicName,
		Key:   key,
		Value: value,
	}
	_, err := p.AppendMessage(msg)
	if err != nil {
		log.Printf("broker: append failed topic=%s key=%s err=%v", topicName, key, err)
		return err
	}

	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
			// Drop if subscriber is slow; SSE should not block producers.
			log.Printf("broker: dropped message for slow subscriber topic=%s key=%s", topicName, key)
		}
	}
	return err
}

func (b *impl) Fetch(topicName string, partition int, offset int64, limit int) ([]storage.Message, error) {
	b.mu.RLock()
	topic, ok := b.topics[topicName]
	b.mu.RUnlock()

	if !ok {
		return nil, errors.New("topic not found")
	}

	if partition < 0 || partition >= len(topic.partitions) {
		return nil, errors.New("partition out of range")
	}

	return topic.partitions[partition].FetchMessages(offset, limit)
}
