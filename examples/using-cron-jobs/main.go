package main

import (
	"context"
	"sync"
	"time"

	"github.com/sllt/pi/pkg/pi"
)

var (
	n  = 0
	mu sync.RWMutex
)

const duration = 2500 * time.Millisecond

func main() {
	app := pi.New()

	// runs every second once the application starts
	app.AddCronJob("* * * * * *", "counter", count)

	// setting the maximum duration of this example application
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	// Cron jobs execute under Pi's supervised runtime after Start/RunContext.
	_ = app.RunContext(ctx)
}

func count(c *pi.Context) {
	mu.Lock()
	defer mu.Unlock()

	n++

	c.Log("Count:", n)
}
