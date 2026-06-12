package lifecycle

import "testing"

func TestManager_DefaultServing(t *testing.T) {
	m := NewManager()
	if m.GetState() != StateServing {
		t.Errorf("default state = %s, want SERVING", m.GetState())
	}
	if m.IsDraining() {
		t.Error("new manager should not be draining")
	}
}

func TestManager_Draining(t *testing.T) {
	m := NewManager()
	m.SetState(StateDraining)
	if m.GetState() != StateDraining {
		t.Errorf("state = %s, want DRAINING", m.GetState())
	}
	if !m.IsDraining() {
		t.Error("IsDraining should be true after SetState(DRAINING)")
	}
}
