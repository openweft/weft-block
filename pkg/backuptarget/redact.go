package backuptarget

import (
	"errors"
	"net/url"
	"strings"
)

// Redact returns a target URL that is safe to log or put in an error: a
// password in the userinfo becomes "xxxxx" (url.URL.Redacted). A string that
// does not parse is reduced to its scheme, because the parser's own error would
// quote the whole thing, userinfo included.
func Redact(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		if i := strings.Index(s, "://"); i >= 0 {
			return s[:i+3] + "<unparseable>"
		}
		return "<unparseable>"
	}
	return u.Redacted()
}

// parseCause strips the URL a *url.Error carries (it quotes the input, which
// may hold a password), keeping only the reason.
func parseCause(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}
