package lifecycle

import (
	"sync"
)

type State string

const (
	StateServing  State = "SERVING"
	StateDraining State = "DRAINING"
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

func (m *Manager) IsDraining() bool {
	return m.GetState() == StateDraining
}
