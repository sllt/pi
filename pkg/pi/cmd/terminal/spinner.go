package terminal

import (
	"context"
	"sync"
	"time"
)

// Spinner is a TUI component that displays a loading spinner which can be used to
// denote a running background process.
type Spinner struct {
	// frames denote the frames of the spinner that displays in continiuation
	frames []string
	// fps is the speed at which the spinner frames are displayed
	fps time.Duration
	// outStream is the Output stream to which the spinner frames are printed onto
	outStream Output

	// started denotes whether the spinner has started spinning and ticker
	// is the time.Ticker for the continuous time update for the spinner.
	mu      sync.Mutex
	started bool
	ticker  *time.Ticker
	stopCh  chan struct{}
	doneCh  chan struct{}
}

func NewDotSpinner(o Output) *Spinner {
	return &Spinner{
		frames:    []string{"⣾ ", "⣽ ", "⣻ ", "⢿ ", "⡿ ", "⣟ ", "⣯ ", "⣷ "},
		fps:       time.Second / 10,
		outStream: o,
	}
}

func NewPulseSpinner(o Output) *Spinner {
	return &Spinner{
		frames:    []string{"█", "▓", "▒", "░"},
		fps:       time.Second / 4,
		outStream: o,
	}
}

func NewGlobeSpinner(o Output) *Spinner {
	return &Spinner{
		frames:    []string{"🌍", "🌎", "🌏"},
		fps:       time.Second / 4,
		outStream: o,
	}
}

func (s *Spinner) Spin(ctx context.Context) *Spinner {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return s
	}
	t := time.NewTicker(s.fps)
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})
	s.ticker = t
	s.started = true
	s.stopCh = stopCh
	s.doneCh = doneCh
	s.mu.Unlock()

	s.outStream.HideCursor()

	go func() {
		defer close(doneCh)
		i := 0
		for {
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-stopCh:
				t.Stop()
				return
			case <-t.C:
				select {
				case <-ctx.Done():
					t.Stop()
					return
				case <-stopCh:
					t.Stop()
					return
				default:
				}
				s.outStream.Print("\r")
				s.outStream.Printf("%s", s.frames[i%len(s.frames)])
				i++
			}
		}
	}()

	return s
}

func (s *Spinner) Stop() {
	s.mu.Lock()
	if !s.started {
		s.mu.Unlock()
		return
	}
	s.started = false
	stopCh := s.stopCh
	doneCh := s.doneCh
	if s.ticker != nil {
		s.ticker.Stop()
	}
	s.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if doneCh != nil {
		<-doneCh
	}

	s.outStream.ClearLine()
	s.outStream.ShowCursor()
	s.outStream.CursorBack(1)
}
