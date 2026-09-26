// A standalone Build application. The host owns signal handling; Build does not.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/config"
)

func main() {
	cfg, err := config.LoadSnapshot("configs", os.Environ())
	if err != nil {
		log.Fatal(err)
	}
	app, err := pi.Build(pi.WithConfig(cfg.Values()))
	if err != nil {
		log.Fatal(err)
	}
	app.GET("/", func(*pi.Context) (any, error) { return map[string]string{"app": "build example"}, nil })
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.RunContext(ctx); err != nil {
		log.Print(err)
	}
}
