package broker

import (
	"fmt"
	"sync"
	"testing"
)

func TestProduceAndFetch(t *testing.T) {
	broker := NewBroker()

	err := broker.Produce("test-topic", "key1", "hello")
	if err != nil {
		t.Fatal(err)
	}

	msgs, err := broker.Fetch("test-topic", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}

	if string(msgs[0].Value) != "hello" {
		t.Fatalf("unexpected value: %s", msgs[0].Value)
	}
}

func TestOffsets(t *testing.T) {
	broker := NewBroker()

	for i := range 5 {
		broker.Produce("t", "", fmt.Sprintf("msg-%d", i))
	}

	msgs, _ := broker.Fetch("t", 0, 2, 2)

	if len(msgs) != 2 {
		t.Fatal("wrong batch size")
	}

	if string(msgs[0].Value) != "msg-2" {
		t.Fatal("wrong offset handling")
	}
}

func TestConcurrentProduce(t *testing.T) {
	broker := NewBroker()

	var wg sync.WaitGroup

	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			broker.Produce("t", "", fmt.Sprintf("msg-%d", i))
		}(i)
	}

	wg.Wait()

	msgs, _ := broker.Fetch("t", 0, 0, 200)

	if len(msgs) != 100 {
		t.Fatalf("expected 100 messages, got %d", len(msgs))
	}
}
