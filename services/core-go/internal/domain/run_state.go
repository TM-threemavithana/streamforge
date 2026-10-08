package domain

import "fmt"

func CanTransition(from, to RunState) bool {
	switch from {
	case RunCreated:
		return to == RunRunning || to == RunCancelled
	case RunRunning:
		return to == RunCompleted || to == RunFailed || to == RunCancelled
	default:
		return false
	}
}

func Transition(run ReplayRun, to RunState) (ReplayRun, error) {
	if !CanTransition(run.State, to) {
		return run, fmt.Errorf("invalid replay run transition %s -> %s", run.State, to)
	}
	run.State = to
	return run, nil
}
