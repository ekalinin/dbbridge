package lifecycle

import (
	"sync"
)

type State string

const (
	StateServing State = "SERVING"
	// StateDraining rejects new queries while the owned ones finish.
	StateDraining State = "DRAINING"
	// StateStoppable is reached once a draining instance has zero in-flight
	// owned queries: the orchestrator may terminate it (I5, spec §10).
	StateStoppable State = "STOPPABLE"
)

type Manager struct {
	mu    sync.RWMutex
	state State
}

func NewManager() *Manager {
	return &Manager{state: StateServing}
}

func (m *Manager) SetState(s State) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = s
}

func (m *Manager) GetState() State {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// IsDraining reports whether the instance has stopped accepting queries. It
// stays true in STOPPABLE, so readiness keeps the node out of rotation right up
// to termination.
func (m *Manager) IsDraining() bool {
	s := m.GetState()
	return s == StateDraining || s == StateStoppable
}

// Advance moves DRAINING to STOPPABLE once the instance has quiesced, and back
// if a query is somehow still counted. It is a no-op while SERVING.
func (m *Manager) Advance(inFlight int) State {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch m.state {
	case StateDraining:
		if inFlight == 0 {
			m.state = StateStoppable
		}
	case StateStoppable:
		if inFlight > 0 {
			m.state = StateDraining
		}
	}
	return m.state
}
