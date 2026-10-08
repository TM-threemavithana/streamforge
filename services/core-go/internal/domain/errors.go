package domain

import "errors"

var (
	ErrInvalidInput  = errors.New("invalid input")
	ErrRunNotRunning = errors.New("replay run is not running")
)
