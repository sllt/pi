package pi

import (
	"testing"

	"github.com/sllt/pi/pkg/pi/config"
)

func TestTelemetryRenamePreservesLegacyOptOut(t *testing.T) {
	for _, tc := range []struct {
		name, current, legacy string
		want                  bool
	}{
		{"legacy opt-out", "", "false", false},
		{"current opt-out", "false", "true", false},
		{"explicit current setting wins", "true", "false", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PI_TELEMETRY", tc.current)
			t.Setenv("KITE_TELEMETRY", tc.legacy)
			app := &App{Config: &config.EnvLoader{}}
			if got := app.hasTelemetry(); got != tc.want {
				t.Fatalf("hasTelemetry() = %v, want %v", got, tc.want)
			}
		})
	}
}
