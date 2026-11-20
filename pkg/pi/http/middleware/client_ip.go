package middleware

import (
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

// ParseTrustedProxies parses a list of IP addresses or CIDR ranges into normalized
// netip.Prefix values (IPv4-mapped IPv6 addresses are unmapped to IPv4).
// Empty or whitespace-only entries are ignored; invalid entries return an error.
func ParseTrustedProxies(entries []string) ([]netip.Prefix, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	prefixes := make([]netip.Prefix, 0, len(entries))
	for _, raw := range entries {
		s := strings.TrimSpace(raw)
		if s == "" {
			continue
		}
		if strings.Contains(s, "/") {
			prefix, err := netip.ParsePrefix(s)
			if err != nil {
				return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", raw, err)
			}
			prefixes = append(prefixes, normalizeTrustedPrefix(prefix))
			continue
		}
		addr, err := netip.ParseAddr(s)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy IP %q: %w", raw, err)
		}
		addr = addr.Unmap()
		bits := 32
		if addr.Is6() {
			bits = 128
		}
		prefixes = append(prefixes, netip.PrefixFrom(addr, bits))
	}
	if len(prefixes) == 0 {
		return nil, nil
	}
	return prefixes, nil
}

func normalizeTrustedPrefix(p netip.Prefix) netip.Prefix {
	addr := p.Addr()
	if addr.Is4In6() && p.Bits() >= 96 {
		return netip.PrefixFrom(addr.Unmap(), p.Bits()-96).Masked()
	}
	return p.Masked()
}

// ExtractClientIP extracts the client IP from r while trusting forwarding headers
// only when the direct peer IP is contained in trusted.
func ExtractClientIP(r *http.Request, trusted []netip.Prefix) string {
	if r == nil {
		return ""
	}
	peerAddr, hasPeerAddr := parseIPToken(r.RemoteAddr)
	if len(trusted) == 0 || !hasPeerAddr || !isTrustedAddr(peerAddr, trusted) {
		if hasPeerAddr {
			return peerAddr.String()
		}
		return fallbackRemoteIP(r.RemoteAddr)
	}

	var xffParts []string
	for _, headerVal := range r.Header.Values("X-Forwarded-For") {
		for _, part := range strings.Split(headerVal, ",") {
			if trimmed := strings.TrimSpace(part); trimmed != "" {
				xffParts = append(xffParts, trimmed)
			}
		}
	}

	var leftmostValid netip.Addr
	hasLeftmostValid := false
	for i := len(xffParts) - 1; i >= 0; i-- {
		addr, ok := parseIPToken(xffParts[i])
		if !ok {
			break
		}
		leftmostValid = addr
		hasLeftmostValid = true
		if !isTrustedAddr(addr, trusted) {
			return addr.String()
		}
	}

	if realAddr, ok := parseIPToken(r.Header.Get("X-Real-IP")); ok {
		return realAddr.String()
	}
	if hasLeftmostValid {
		return leftmostValid.String()
	}
	return peerAddr.String()
}

func isTrustedAddr(addr netip.Addr, trusted []netip.Prefix) bool {
	if !addr.IsValid() || len(trusted) == 0 {
		return false
	}
	unmapped := addr.Unmap().WithZone("")
	for _, prefix := range trusted {
		if prefix.Contains(unmapped) {
			return true
		}
	}
	return false
}

func parseIPToken(raw string) (netip.Addr, bool) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return netip.Addr{}, false
	}
	if addr, err := netip.ParseAddr(s); err == nil {
		return addr.Unmap().WithZone(""), true
	}
	if ap, err := netip.ParseAddrPort(s); err == nil {
		return ap.Addr().Unmap().WithZone(""), true
	}
	if strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]") {
		if addr, err := netip.ParseAddr(s[1 : len(s)-1]); err == nil {
			return addr.Unmap().WithZone(""), true
		}
	}
	return netip.Addr{}, false
}

func fallbackRemoteIP(remoteAddr string) string {
	trimmed := strings.TrimSpace(remoteAddr)
	if trimmed == "" {
		return ""
	}
	if host, _, err := net.SplitHostPort(trimmed); err == nil {
		return host
	}
	return trimmed
}
