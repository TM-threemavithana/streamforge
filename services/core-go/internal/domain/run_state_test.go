package domain

import "testing"

func TestAllowedRunStateTransitions(t *testing.T) {
	tests := []struct {
		from RunState
		to   RunState
	}{
		{RunCreated, RunRunning},
		{RunCreated, RunCancelled},
		{RunRunning, RunCompleted},
		{RunRunning, RunFailed},
		{RunRunning, RunCancelled},
	}
	for _, test := range tests {
		if !CanTransition(test.from, test.to) {
			t.Fatalf("expected %s -> %s to be allowed", test.from, test.to)
		}
	}
}

func TestTerminalRunStatesCannotTransition(t *testing.T) {
	for _, state := range []RunState{RunCompleted, RunFailed, RunCancelled} {
		if CanTransition(state, RunRunning) {
			t.Fatalf("expected %s to be terminal", state)
		}
	}
}
