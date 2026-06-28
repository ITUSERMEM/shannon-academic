package handlers

import (
	"encoding/json"
	"net/http"
	"sync"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/degradation"
	"go.uber.org/zap"
)

type CircuitBreakerEntry struct {
	Name   string                `json:"name"`
	State  string                `json:"state"`
	Counts circuitbreaker.Counts `json:"counts"`
}

type CircuitBreakerStatusHandler struct {
	mu       sync.RWMutex
	breakers map[string]*circuitbreaker.CircuitBreaker
	logger   *zap.Logger
}

func NewCircuitBreakerStatusHandler(logger *zap.Logger) *CircuitBreakerStatusHandler {
	return &CircuitBreakerStatusHandler{
		breakers: make(map[string]*circuitbreaker.CircuitBreaker),
		logger:   logger,
	}
}

func (h *CircuitBreakerStatusHandler) Register(name string, cb *circuitbreaker.CircuitBreaker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.breakers[name] = cb
}

func (h *CircuitBreakerStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	entries := make([]CircuitBreakerEntry, 0, len(h.breakers))
	for name, cb := range h.breakers {
		entries = append(entries, CircuitBreakerEntry{
			Name:   name,
			State:  cb.State().String(),
			Counts: cb.Counts(),
		})
	}

	if entries == nil {
		entries = []CircuitBreakerEntry{}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"circuit_breakers": entries,
	})
}

type DegradationLevelHandler struct {
	manager *degradation.Manager
	logger  *zap.Logger
}

func NewDegradationLevelHandler(manager *degradation.Manager, logger *zap.Logger) *DegradationLevelHandler {
	return &DegradationLevelHandler{
		manager: manager,
		logger:  logger,
	}
}

func (h *DegradationLevelHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	level := degradation.LevelNone
	overall := "none"
	if h.manager != nil {
		isDegraded, lvl, err := h.manager.IsSystemDegraded(ctx)
		if err != nil {
			h.logger.Warn("Failed to check degradation level", zap.Error(err))
		} else {
			level = lvl
			if isDegraded {
				overall = level.String()
			}
		}
	}

	status := "normal"
	if level >= degradation.LevelMinor {
		status = "degraded"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"level":   level,
		"status":  status,
		"overall": overall,
	})
}
