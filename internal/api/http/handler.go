package httpapi

import (
	"bob-the-broker/internal/broker"
	"encoding/json"
	"fmt"
	"net/http"
)

type Handler struct {
	broker broker.Broker
}

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

	ch := h.broker.Subscribe(topic)
	defer h.broker.Unsubscribe(topic, ch)

	for {
		select {
		case <-r.Context().Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			data := fmt.Sprintf(`data: {"topic":"%s","key":"%s","value":"%s"}\n\n`,
				msg.Topic, msg.Key, msg.Value)
			fmt.Fprint(w, data)
			flusher.Flush()
		}
	}
}

func (h *Handler) Produce(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Topic string `json:"topic"`
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	err := h.broker.Produce(req.Topic, req.Key, req.Value)
	if err != nil {
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
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	msgs, err := h.broker.Fetch(req.Topic, req.Partition, req.Offset, req.Limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(msgs)
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/produce", h.Produce)
	mux.HandleFunc("/fetch", h.Fetch)
	return mux
}
