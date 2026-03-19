package httpapi

import (
	"encoding/json"
	"net/http"

	"bob-the-broker/internal/broker"
)

type Handler struct {
	broker broker.Broker
}

func NewHandler(b broker.Broker) *Handler {
	return &Handler{
		broker: b,
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
