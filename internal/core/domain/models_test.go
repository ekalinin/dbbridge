package domain

import (
	"testing"
)

func TestQueryStateTransitions(t *testing.T) {
	tests := []struct {
		current QueryState
		next    QueryState
		valid   bool
	}{
		{StatePending, StateRunning, true},
		{StatePending, StateCanceled, true},
		{StatePending, StateFailed, true},
		{StatePending, StateExpired, false},

		{StateRunning, StateSucceeded, true},
		{StateRunning, StateFailed, true},
		{StateRunning, StateCanceled, true},
		{StateRunning, StatePending, false},

		{StateSucceeded, StateExpired, true},
		{StateSucceeded, StateRunning, false},

		{StateExpired, StatePending, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.current)+"->"+string(tt.next), func(t *testing.T) {
			got := tt.current.CanTransitionTo(tt.next)
			if got != tt.valid {
				t.Errorf("expected %s.CanTransitionTo(%s) = %v; got %v", tt.current, tt.next, tt.valid, got)
			}
		})
	}
}

func TestQueryStateTerminal(t *testing.T) {
	tests := []struct {
		state    QueryState
		terminal bool
	}{
		{StatePending, false},
		{StateRunning, false},
		{StateSucceeded, true},
		{StateFailed, true},
		{StateCanceled, true},
		{StateExpired, true},
	}

	for _, tt := range tests {
		t.Run(string(tt.state), func(t *testing.T) {
			got := tt.state.IsTerminal()
			if got != tt.terminal {
				t.Errorf("expected %s.IsTerminal() = %v; got %v", tt.state, tt.terminal, got)
			}
		})
	}
}
