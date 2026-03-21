package httpapi

import (
	"bob-the-broker/internal/broker"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type Handler struct {
	broker broker.Broker
}

const (
	maxRequestBodyBytes = 1 * 1024 * 1024
	maxValueBytes       = 256 * 1024
	maxTopicLen         = 128
	maxKeyLen           = 256
)

func NewHandler(b broker.Broker) *Handler {
	return &Handler{
		broker: b,
	}
}

func (h *Handler) SseSubscribe(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic query parameter is required", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	log.Printf("sse: subscribe topic=%s remote=%s", topic, r.RemoteAddr)
	ch := h.broker.Subscribe(topic)
	defer h.broker.Unsubscribe(topic, ch)
	defer log.Printf("sse: unsubscribe topic=%s remote=%s", topic, r.RemoteAddr)

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	rc := http.NewResponseController(w)
	for {
		select {
		case <-r.Context().Done():
			log.Printf("sse: context done topic=%s remote=%s err=%v", topic, r.RemoteAddr, r.Context().Err())
			return
		case msg, ok := <-ch:
			if !ok {
				log.Printf("sse: channel closed topic=%s remote=%s", topic, r.RemoteAddr)
				return
			}

			payload := struct {
				Topic string `json:"topic"`
				Key   string `json:"key"`
				Value string `json:"value"`
			}{
				Topic: msg.Topic,
				Key:   msg.Key,
				Value: msg.Value,
			}
			b, err := json.Marshal(payload)
			if err != nil {
				log.Printf("sse: marshal failed topic=%s key=%s err=%v", msg.Topic, msg.Key, err)
				continue
			}
			if _, err := io.WriteString(w, fmt.Sprintf("data: %s\n\n", b)); err != nil {
				log.Printf("sse: write failed topic=%s remote=%s err=%v", topic, r.RemoteAddr, err)
				return
			}
			flusher.Flush()
		case <-ticker.C:
			err := rc.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err != nil {
				log.Printf("deadline error: %v", err)
				return
			}

			if _, err := io.WriteString(w, "ping\n\n"); err != nil {
				log.Printf("failed to ping")
				return
			}
			flusher.Flush()
		}
	}
}

func (h *Handler) Produce(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	var req struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("produce: decode failed remote=%s err=%v", r.RemoteAddr, err)
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if len(req.Topic) == 0 || len(req.Topic) > maxTopicLen {
		http.Error(w, "invalid topic", http.StatusBadRequest)
		return
	}
	if len(req.Key) > maxKeyLen {
		http.Error(w, "key too long", http.StatusBadRequest)
		return
	}
	if len(req.Value) > maxValueBytes {
		http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
		return
	}

	err := h.broker.Produce(req.Topic, req.Key, req.Value)
	if err != nil {
		log.Printf("produce: broker error topic=%s key=%s remote=%s err=%v", req.Topic, req.Key, r.RemoteAddr, err)
		http.Error(w, "failed to produce message", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (h *Handler) Fetch(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic     string `json:"topic"`
		Partition int    `json:"partition"`
		Offset    int64  `json:"offset"`
		Limit     int    `json:"limit"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("fetch: decode failed remote=%s err=%v", r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msgs, err := h.broker.Fetch(req.Topic, req.Partition, req.Offset, req.Limit)
	if err != nil {
		log.Printf("fetch: broker error topic=%s partition=%d offset=%d limit=%d remote=%s err=%v",
			req.Topic, req.Partition, req.Offset, req.Limit, r.RemoteAddr, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(msgs); err != nil {
		log.Printf("fetch: encode failed topic=%s remote=%s err=%v", req.Topic, r.RemoteAddr, err)
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/produce", h.Produce)
	mux.HandleFunc("/fetch", h.Fetch)
	mux.HandleFunc("/subscribe", h.SseSubscribe)
	return mux
}
