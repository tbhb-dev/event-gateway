// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// Package redelivery supports GitHub webhook redelivery.
package redelivery

import (
	"bytes"
	"cmp"
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

const (
	redacted      = "[redacted]"
	responseLimit = 1 << 20
	refreshBefore = 2 * time.Minute
	jwtLifetime   = 9 * time.Minute
)

// Options configures GitHub App authentication and selects an organization webhook. An empty BaseURL uses
// https://api.github.com. A nil HTTPClient uses http.DefaultClient, and a nil
// Now uses time.Now.
type Options struct {
	HTTPClient     *http.Client
	Now            func() time.Time
	AppID          string
	KeyFile        string
	BaseURL        string
	Org            string
	InstallationID int64
	HookID         int64
}

// Client exchanges GitHub App credentials for installation tokens.
//
//nolint:recvcheck // Value receivers redact copies, and authentication methods operate on the shared client.
type Client struct {
	key            *rsa.PrivateKey
	cache          *tokenCache
	path           string
	deliveriesPath string
	options        Options
}

var orgPattern = regexp.MustCompile(`^[A-Za-z0-9-]+$`)

type tokenCache struct {
	token installationToken
	mu    sync.Mutex
}

type installationToken struct {
	ExpiresAt time.Time `json:"expires_at"` //nolint:tagliatelle // GitHub names this field expires_at.
	Token     string    `json:"token"`
}

func (installationToken) String() string       { return redacted }
func (installationToken) GoString() string     { return redacted }
func (installationToken) LogValue() slog.Value { return slog.StringValue(redacted) }

// NewClient loads the RSA private key from disk once.
//
//nolint:err113 // Key diagnostics report the configured path and discard parser and system details.
func NewClient(options Options) (*Client, error) {
	if !orgPattern.MatchString(options.Org) || options.HookID <= 0 {
		return nil, errors.New("GitHub org must use ASCII letters, digits or hyphens, and hook ID must be positive")
	}

	data, err := os.ReadFile(options.KeyFile)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("read GitHub App key %s: %w", options.KeyFile, fs.ErrNotExist)
	}

	if err != nil {
		return nil, fmt.Errorf("read GitHub App key %s", options.KeyFile)
	}

	key := parseKey(data)
	if key == nil {
		return nil, fmt.Errorf("parse GitHub App RSA key %s", options.KeyFile)
	}

	if options.Now == nil {
		options.Now = time.Now
	}

	client := *cmp.Or(options.HTTPClient, http.DefaultClient)
	// Keep credentials on the configured endpoints.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	options.HTTPClient = &client
	options.BaseURL = strings.TrimRight(cmp.Or(options.BaseURL, "https://api.github.com"), "/")

	return &Client{
		key: key, cache: new(tokenCache), options: options,
		path: fmt.Sprintf("/app/installations/%d/access_tokens", options.InstallationID),

		deliveriesPath: fmt.Sprintf("/orgs/%s/hooks/%d/deliveries", options.Org, options.HookID),
	}, nil
}

// String redacts credentials in formatted output.
func (Client) String() string { return redacted }

// GoString redacts credentials in Go-syntax output.
func (Client) GoString() string { return redacted }

// LogValue redacts credentials in structured logs.
func (Client) LogValue() slog.Value { return slog.StringValue(redacted) }

func parseKey(data []byte) *rsa.PrivateKey {
	block, rest := pem.Decode(data)
	if block == nil || len(bytes.TrimSpace(rest)) != 0 || len(block.Headers) != 0 {
		return nil
	}

	switch block.Type {
	case "RSA PRIVATE KEY":
		//nolint:errcheck // The parser returns a nil key on failure, and the error may include key details.
		key, _ := x509.ParsePKCS1PrivateKey(block.Bytes)

		return key
	case "PRIVATE KEY":
		//nolint:errcheck // A failed parse or a non-RSA key produces a nil RSA key below.
		key, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
		//nolint:errcheck // A non-RSA key is rejected by returning nil.
		rsaKey, _ := key.(*rsa.PrivateKey)

		return rsaKey
	}

	return nil
}

// Invalidate drops the cached installation token.
func (c *Client) Invalidate() {
	c.cache.mu.Lock()
	defer c.cache.mu.Unlock()

	c.cache.token = installationToken{ExpiresAt: time.Time{}, Token: ""}
}

// Token returns an installation token, refreshing it two minutes before expiry.
//
//nolint:err113 // Exchange diagnostics retain the method, path and status and discard secret-bearing errors.
func (c *Client) Token(ctx context.Context) (string, error) {
	cache := c.cache
	cache.mu.Lock()
	defer cache.mu.Unlock()

	now := c.options.Now()
	if now.Before(cache.token.ExpiresAt.Add(-refreshBefore)) {
		return cache.token.Token, nil
	}

	jwt, ok := c.sign(now)
	if !ok {
		return "", fmt.Errorf("POST %s: sign GitHub App JWT", c.path)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.options.BaseURL+c.path, nil)
	if err != nil {
		return "", fmt.Errorf("POST %s: create token request", c.path)
	}

	setGitHubHeaders(req, jwt)

	res, err := c.options.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("POST %s: send token request", c.path)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("POST %s: HTTP %d", c.path, res.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(res.Body, responseLimit+1))

	var token installationToken
	if err != nil || len(data) > responseLimit || json.Unmarshal(data, &token) != nil ||
		token.Token == "" || token.ExpiresAt.IsZero() || !token.ExpiresAt.After(now.Add(refreshBefore)) {
		return "", fmt.Errorf("POST %s: HTTP %d invalid token response", c.path, res.StatusCode)
	}

	cache.token = token

	return token.Token, nil
}

func setGitHubHeaders(req *http.Request, token string) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-Github-Api-Version", "2022-11-28")
	req.Header.Set("User-Agent", "event-gateway")
	req.Header.Set("Authorization", "Bearer "+token)
}

func (c *Client) sign(now time.Time) (string, bool) {
	const header = `{"alg":"RS256","typ":"JWT"}`

	//nolint:errcheck,errchkjson // JSON marshaling a string cannot fail.
	issuer, _ := json.Marshal(c.options.AppID)
	claims := fmt.Sprintf(
		`{"iss":%s,"iat":%d,"exp":%d}`, issuer, now.Add(-time.Minute).Unix(), now.Add(jwtLifetime).Unix(),
	)
	encoding := base64.RawURLEncoding
	unsigned := encoding.EncodeToString([]byte(header)) + "." + encoding.EncodeToString([]byte(claims))
	digest := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(nil, c.key, crypto.SHA256, digest[:])

	return unsigned + "." + encoding.EncodeToString(sig), err == nil
}
