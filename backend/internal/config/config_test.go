package config

import "testing"

func TestValidateRejectsInvalidSnowflakeNodeID(t *testing.T) {
	for _, nodeID := range []int64{-1, 1024} {
		cfg := Config{EmailMode: "log", SnowflakeNodeID: nodeID}
		if err := cfg.Validate(); err == nil {
			t.Fatalf("node ID %d was accepted", nodeID)
		}
	}
}
