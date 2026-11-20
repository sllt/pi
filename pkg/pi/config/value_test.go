package config_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi/config"
)

func TestValue_RawIsSetString(t *testing.T) {
	assert.Equal(t, "", config.ValueOf("").Raw())
	assert.False(t, config.ValueOf("").IsSet())
	assert.False(t, config.ValueOf("   \t\n ").IsSet())
	assert.Equal(t, "   ", config.ValueOf("   ").Raw())
	assert.Equal(t, "", config.ValueOf("").String())
	assert.Equal(t, "fallback", config.ValueOf("").String("fallback"))
	assert.Equal(t, "fallback", config.ValueOf("  ").String("fallback"))

	v := config.ValueOf(" hello ")
	assert.True(t, v.IsSet())
	assert.Equal(t, " hello ", v.Raw())
	assert.Equal(t, " hello ", v.String("fallback"))
}

func TestValue_IntAndInt64(t *testing.T) {
	assert.Equal(t, 0, config.ValueOf("").Int())
	assert.Equal(t, 8080, config.ValueOf("").Int(8080))
	assert.Equal(t, 8080, config.ValueOf("  ").Int(8080))
	assert.Equal(t, 9090, config.ValueOf(" 9090 ").Int(8080))
	assert.Equal(t, 8080, config.ValueOf("not-an-int").Int(8080))
	assert.Equal(t, 0, config.ValueOf("not-an-int").Int())

	n, err := config.ValueOf("").IntE(8080)
	require.NoError(t, err)
	assert.Equal(t, 8080, n)

	n, err = config.ValueOf("").IntE()
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	n, err = config.ValueOf(" -42 ").IntE(8080)
	require.NoError(t, err)
	assert.Equal(t, -42, n)

	_, err = config.ValueOf("abc").IntE(8080)
	require.Error(t, err)

	assert.Equal(t, int64(0), config.ValueOf("").Int64())
	assert.Equal(t, int64(1<<40), config.ValueOf(" 1099511627776 ").Int64(123))
	assert.Equal(t, int64(123), config.ValueOf("bad").Int64(123))

	n64, err := config.ValueOf("").Int64E(55)
	require.NoError(t, err)
	assert.Equal(t, int64(55), n64)

	n64, err = config.ValueOf("9876543210").Int64E()
	require.NoError(t, err)
	assert.Equal(t, int64(9876543210), n64)

	_, err = config.ValueOf("99.5").Int64E(55)
	require.Error(t, err)
}

func TestValue_Float64(t *testing.T) {
	assert.Equal(t, 0.0, config.ValueOf("").Float64())
	assert.InDelta(t, 1.5, config.ValueOf("").Float64(1.5), 1e-9)
	assert.InDelta(t, 3.14, config.ValueOf(" 3.14 ").Float64(1.5), 1e-9)
	assert.InDelta(t, 1.5, config.ValueOf("nope").Float64(1.5), 1e-9)

	f, err := config.ValueOf("").Float64E(2.5)
	require.NoError(t, err)
	assert.InDelta(t, 2.5, f, 1e-9)

	f, err = config.ValueOf("-0.25").Float64E(2.5)
	require.NoError(t, err)
	assert.InDelta(t, -0.25, f, 1e-9)

	_, err = config.ValueOf("abc").Float64E(2.5)
	require.Error(t, err)
}

func TestValue_Bool(t *testing.T) {
	assert.False(t, config.ValueOf("").Bool())
	assert.True(t, config.ValueOf("").Bool(true))
	assert.True(t, config.ValueOf("  ").Bool(true))
	assert.True(t, config.ValueOf(" true ").Bool(false))
	assert.True(t, config.ValueOf("1").Bool(false))
	assert.False(t, config.ValueOf("0").Bool(true))
	assert.False(t, config.ValueOf("FALSE").Bool(true))
	assert.True(t, config.ValueOf("invalid").Bool(true))

	b, err := config.ValueOf("").BoolE(true)
	require.NoError(t, err)
	assert.True(t, b)

	b, err = config.ValueOf("t").BoolE(false)
	require.NoError(t, err)
	assert.True(t, b)

	_, err = config.ValueOf("maybe").BoolE(true)
	require.Error(t, err)
}

func TestValue_Duration(t *testing.T) {
	assert.Equal(t, time.Duration(0), config.ValueOf("").Duration())
	assert.Equal(t, 5*time.Second, config.ValueOf("").Duration(5*time.Second))
	assert.Equal(t, 250*time.Millisecond, config.ValueOf(" 250ms ").Duration(5*time.Second))
	assert.Equal(t, 5*time.Second, config.ValueOf("bad").Duration(5*time.Second))

	d, err := config.ValueOf("").DurationE(5 * time.Second)
	require.NoError(t, err)
	assert.Equal(t, 5*time.Second, d)

	d, err = config.ValueOf("2m30s").DurationE()
	require.NoError(t, err)
	assert.Equal(t, 150*time.Second, d)

	_, err = config.ValueOf("10").DurationE(5 * time.Second)
	require.Error(t, err)
}

func TestValue_Strings(t *testing.T) {
	assert.Nil(t, config.ValueOf("").Strings())
	assert.Nil(t, config.ValueOf(" , , ").Strings())
	assert.Equal(t, []string{"a", "b", "c"}, config.ValueOf(" a , , b,c , ").Strings())

	def := []string{"127.0.0.1", "::1"}
	got := config.ValueOf("").Strings(def)
	assert.Equal(t, def, got)
	got[0] = "mutated"
	assert.Equal(t, []string{"127.0.0.1", "::1"}, def, "Strings must return a copy of default slice")

	gotEmptyTokens := config.ValueOf(" , , ").Strings(def)
	assert.Equal(t, def, gotEmptyTokens)
	gotEmptyTokens[1] = "mutated"
	assert.Equal(t, []string{"127.0.0.1", "::1"}, def)
}

func TestEnvLookupAndSnapshotEnv(t *testing.T) {
	t.Setenv("PI_TEST_VALUE_KEY", "42")
	assert.Equal(t, 42, config.Env("PI_TEST_VALUE_KEY").Int())
	assert.Equal(t, 99, config.Env("PI_TEST_VALUE_MISSING").Int(99))

	assert.Equal(t, "", config.Lookup(nil, "K").Raw())
	var nilSnap *config.Snapshot
	assert.Equal(t, "", config.Lookup(nilSnap, "K").Raw())
	assert.Equal(t, "def", nilSnap.Env("K").String("def"))

	snap := config.NewSnapshot(map[string]string{
		"RATE_LIMIT": "100",
		"ENABLED":    "true",
		"TIMEOUT":    "3s",
	})
	assert.Equal(t, 100, config.Lookup(snap, "RATE_LIMIT").Int())
	assert.True(t, snap.Env("ENABLED").Bool())
	assert.Equal(t, 3*time.Second, snap.Env("TIMEOUT").Duration())
	assert.False(t, snap.Env("MISSING").IsSet())
}

func BenchmarkValue_Parsers(b *testing.B) {
	vInt := config.ValueOf("8080")
	vBool := config.ValueOf("true")
	vDur := config.ValueOf("15s")
	vFloat := config.ValueOf("3.14")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = vInt.Int(80)
		_ = vBool.Bool()
		_ = vDur.Duration()
		_ = vFloat.Float64()
	}
}
