package main

import "testing"

func TestRequireLoopbackRejectsExternallyReachableBinds(t *testing.T) {
	for _, address := range []string{"0.0.0.0:8080", ":8080", "192.168.1.10:8080"} {
		if err := requireLoopback("HTTP", address); err == nil {
			t.Fatalf("expected %q to be rejected", address)
		}
	}
	for _, address := range []string{"127.0.0.1:8080", "localhost:8080", "[::1]:8080"} {
		if err := requireLoopback("HTTP", address); err != nil {
			t.Fatalf("expected %q to be accepted: %v", address, err)
		}
	}
}
