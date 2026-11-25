// Package secretenv lets a secret that used to be an environment variable be
// given by reference instead.
//
// For each secret NAME the operator may set NAME (plaintext, deprecated: it is
// readable in /proc/PID/environ and inherited by every child process) or
// NAME_FROM, a github.com/go-authn/secretsource reference (systemd-creds:,
// keychain:, keepassxc:, file:). Both set is an error. [Load] reads the value
// the first time it is asked for, keeps it in memory and unsets the plaintext
// variable so that child processes do not inherit it. The names are only known
// when a request arrives (a backup spec names its own variable), hence a lazy
// load rather than a startup step. Errors never contain the value.
package secretenv

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/go-authn/secretsource"
)

// ErrUnset reports that neither NAME nor NAME_FROM is set (or both are empty).
var ErrUnset = errors.New("secretenv: not set")

// Warn receives deprecation notices. Tests replace it.
var Warn io.Writer = os.Stderr

// open is replaced by tests.
var open = func(ctx context.Context, ref string) ([]byte, error) {
	s, err := secretsource.Open(ctx, ref)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	return append([]byte(nil), s.Bytes()...), nil
}

var (
	mu     sync.Mutex
	values = map[string][]byte{}
	wmu    sync.Mutex
	warned = map[string]bool{}
)

// Load returns the secret NAME as a fresh slice the caller may wipe.
func Load(ctx context.Context, name string) ([]byte, error) {
	return LoadHint(ctx, name, name+"_FROM")
}

// LoadHint is [Load] with the deprecation notice naming hint as the variable
// to use instead of NAME_FROM (for a plaintext variable a library owns, such
// as AWS_SECRET_ACCESS_KEY, whose replacement has another name).
func LoadHint(ctx context.Context, name, hint string) ([]byte, error) {
	mu.Lock()
	defer mu.Unlock()
	plain := os.Getenv(name)
	ref := os.Getenv(name + "_FROM")
	if ref != "" && plain != "" {
		return nil, fmt.Errorf("%s and %s_FROM are both set; use only %s_FROM", name, name, name)
	}
	switch {
	case ref != "":
		return fromRef(ctx, name, ref)
	case plain != "":
		DeprecatedEnv(name, hint)
		values[name] = []byte(plain)
		_ = os.Unsetenv(name)
		return []byte(plain), nil
	}
	// the plaintext variable was captured, then unset, by an earlier call.
	if v, ok := values[name]; ok {
		return append([]byte(nil), v...), nil
	}
	return nil, ErrUnset
}

// fromRef resolves NAME_FROM=ref, cached per (name, ref). The caller holds mu.
func fromRef(ctx context.Context, name, ref string) ([]byte, error) {
	key := name + "_FROM\x00" + ref
	if v, ok := values[key]; ok {
		return append([]byte(nil), v...), nil
	}
	raw, err := open(ctx, ref)
	if err != nil {
		return nil, fmt.Errorf("%s_FROM: %w", name, err)
	}
	v := []byte(strings.TrimRight(string(raw), "\r\n"))
	clear(raw)
	if len(v) == 0 {
		return nil, fmt.Errorf("%s_FROM: the secret is empty", name)
	}
	values[key] = v
	return append([]byte(nil), v...), nil
}

// DeprecatedEnv writes, once per name, the notice that the plaintext variable
// is deprecated in favour of the named replacement.
func DeprecatedEnv(name, replacement string) {
	WarnOnce(name, fmt.Sprintf("%s is deprecated: a plaintext environment variable is readable in /proc/PID/environ and inherited by child processes; use %s=<ref> (systemd-creds:, keychain:, keepassxc:, file:)", name, replacement))
}

// WarnOnce writes msg to [Warn] the first time key is seen.
func WarnOnce(key, msg string) {
	wmu.Lock()
	defer wmu.Unlock()
	if warned[key] {
		return
	}
	warned[key] = true
	fmt.Fprintln(Warn, msg)
}

// LoadFrom reads a secret that exists ONLY by reference: NAME_FROM. There is
// no plaintext NAME form. [ErrUnset] when NAME_FROM is not set.
func LoadFrom(ctx context.Context, name string) ([]byte, error) {
	mu.Lock()
	defer mu.Unlock()
	ref := os.Getenv(name + "_FROM")
	if ref == "" {
		return nil, ErrUnset
	}
	return fromRef(ctx, name, ref)
}

// Reset forgets every stored secret and every once-only warning. For tests of
// packages that load secrets through this one: the cache is process-wide.
func Reset() { reset() }

// reset forgets every stored secret (tests).
func reset() {
	mu.Lock()
	for _, v := range values {
		clear(v)
	}
	clear(values)
	mu.Unlock()
	wmu.Lock()
	clear(warned)
	wmu.Unlock()
}
