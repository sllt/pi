package logging

import (
	"context"
	"log/slog"
	"slices"
)

type slogGroupOrAttrs struct {
	group string
	attrs []slog.Attr
}

type slogHandler struct {
	base   Logger
	frames []slogGroupOrAttrs
}

// NewSlogHandler adapts a Pi logging.Logger to log/slog.Handler without touching slog.Default().
func NewSlogHandler(base Logger) slog.Handler {
	if isNilLogger(base) {
		base = nil
	}
	return &slogHandler{base: base}
}

func (h *slogHandler) Enabled(_ context.Context, level slog.Level) bool {
	if h == nil || isNilLogger(h.base) {
		return false
	}
	unwrapped := unwrapBaseLogger(h.base)
	if unwrapped == nil {
		return false
	}
	if lg, ok := unwrapped.(levelGetter); ok {
		return slogLevelToPiLevel(level) >= lg.getLevel()
	}
	return true
}

func (h *slogHandler) Handle(ctx context.Context, r slog.Record) error {
	if h == nil || isNilLogger(h.base) || !h.Enabled(ctx, r.Level) {
		return nil
	}

	fields := make(map[string]any)
	prefix := ""
	for _, frame := range h.frames {
		if frame.group != "" {
			if prefix == "" {
				prefix = frame.group
			} else {
				prefix += "." + frame.group
			}
			continue
		}
		for _, a := range frame.attrs {
			appendSlogAttrToMap(fields, prefix, a)
		}
	}
	r.Attrs(func(a slog.Attr) bool {
		appendSlogAttrToMap(fields, prefix, a)
		return true
	})

	cl := WithContext(ctx, h.base)
	cl.logStructuredMapWithCaller(slogLevelToPiLevel(r.Level), r.PC, r.Message, fields, true)
	return nil
}

func (h *slogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h == nil || len(attrs) == 0 {
		return h
	}
	frames := make([]slogGroupOrAttrs, len(h.frames)+1)
	copy(frames, h.frames)
	frames[len(h.frames)] = slogGroupOrAttrs{attrs: slices.Clone(attrs)}
	return &slogHandler{
		base:   h.base,
		frames: frames,
	}
}

func (h *slogHandler) WithGroup(name string) slog.Handler {
	if h == nil || name == "" {
		return h
	}
	frames := make([]slogGroupOrAttrs, len(h.frames)+1)
	copy(frames, h.frames)
	frames[len(h.frames)] = slogGroupOrAttrs{group: name}
	return &slogHandler{
		base:   h.base,
		frames: frames,
	}
}

func slogLevelToPiLevel(l slog.Level) Level {
	switch {
	case l >= slog.LevelError:
		return ERROR
	case l >= slog.LevelWarn:
		return WARN
	case l >= slog.LevelInfo:
		return INFO
	default:
		return DEBUG
	}
}

func appendSlogAttrToMap(dst map[string]any, groupPrefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		nextPrefix := a.Key
		if groupPrefix != "" && nextPrefix != "" {
			nextPrefix = groupPrefix + "." + nextPrefix
		} else if groupPrefix != "" {
			nextPrefix = groupPrefix
		}
		for _, sub := range a.Value.Group() {
			appendSlogAttrToMap(dst, nextPrefix, sub)
		}
		return
	}
	key := a.Key
	if groupPrefix != "" && key != "" {
		key = groupPrefix + "." + key
	}
	dst[key] = slogValueToAny(a.Value)
}

func slogValueToAny(v slog.Value) any {
	switch v.Kind() {
	case slog.KindString:
		return v.String()
	case slog.KindInt64:
		return v.Int64()
	case slog.KindUint64:
		return v.Uint64()
	case slog.KindFloat64:
		return normalizeFieldValue(v.Float64())
	case slog.KindBool:
		return v.Bool()
	case slog.KindDuration:
		return v.Duration()
	case slog.KindTime:
		return v.Time()
	case slog.KindAny:
		return normalizeFieldValue(v.Any())
	default:
		return normalizeFieldValue(v.Any())
	}
}
