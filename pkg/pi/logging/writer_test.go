package logging

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriterLoggerRoutesOutput(t *testing.T) {
	var normal, failures bytes.Buffer
	l := NewWriterLogger(INFO, &normal, &failures)
	l.Info("migration info")
	l.Error("migration error")
	require.Contains(t, normal.String(), "migration info")
	require.NotContains(t, normal.String(), "migration error")
	require.Contains(t, failures.String(), "migration error")
	l = NewWriterLogger(DEBUG, nil, nil)
	require.NotPanics(t, func() { l.Info("discarded"); l.Error("discarded") })
}
