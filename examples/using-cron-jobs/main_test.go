package main

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/testutil"
)

func TestMain(m *testing.M) {
	os.Setenv("PI_TELEMETRY", "false")
	m.Run()
}

func Test_UserPurgeCron(t *testing.T) {
	configs := testutil.NewServerConfigs(t)

	mu.Lock()
	n = 0
	mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		main()
	}()

	require.Eventually(t, func() bool {
		mu.RLock()
		defer mu.RUnlock()
		return n >= 1
	}, 2200*time.Millisecond, 25*time.Millisecond)

	<-done
	t.Logf("Metrics server ran at: %s", configs.MetricsHost)
}
