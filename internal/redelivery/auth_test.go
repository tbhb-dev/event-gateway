// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// cspell:ignore testpackage

package redelivery //nolint:testpackage // Exercise redaction and signer failures through private state.

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	tokenPath   = "/app/installations/42/access_tokens" //nolint:gosec // This is a fake API path.
	secretToken = "test-installation-secret"
	secretKey   = "test-key-material"
	testAppID   = "12345"
)

//nolint:cyclop,gocognit // The full request and cache sequence is checked in one scenario.
func TestTokenCacheAndJWT(t *testing.T) {
	t.Parallel()

	key := testKey(t)
	file := writeKey(t, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	start := time.Unix(0, 0).UTC()
	now, expiry := start, start.Add(time.Hour).Format(time.RFC3339)

	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.RequestURI() != tokenPath || req.ContentLength != 0 ||
			req.Header.Get("Accept") != "application/vnd.github+json" || req.UserAgent() != "event-gateway" ||
			req.Header.Get("X-Github-Api-Version") != "2022-11-28" {
			t.Error("unexpected installation token request")
		}

		call := calls.Add(1)
		if call == 1 {
			verifyJWT(t, strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer "), &key.PublicKey, start)
		}

		writer.WriteHeader(http.StatusCreated)

		_, err := fmt.Fprintf(writer, `{"token":%q,"expires_at":%q}`, fmt.Sprint(secretToken, call), expiry)
		if err != nil {
			t.Error("write token response")
		}
	}))
	t.Cleanup(server.Close)

	clock := func() time.Time { return now }
	client := mustClient(t, Options{server.Client(), clock, testAppID, file, server.URL + "/", 42})

	for _, want := range []int32{1, 1, 2, 3} {
		token, err := client.Token(t.Context())
		if err != nil || token != fmt.Sprint(secretToken, want) || calls.Load() != want {
			t.Fatalf("token exchange %d failed: %v", want, err)
		}

		switch want {
		case 1:
			now = now.Add(29 * time.Minute)
		case 2:
			client.Invalidate()
		}
	}

	var logs bytes.Buffer

	logger := slog.New(slog.NewTextHandler(&logs, nil))
	logger.InfoContext(t.Context(), "credentials", "client", client, "record", client.cache.token)

	for _, value := range []any{client, *client, client.cache.token} {
		if fmt.Sprintf("%v %+v %#v", value, value, value) != strings.Repeat(redacted+" ", 2)+redacted {
			t.Fatal("formatter exposed credentials")
		}
	}

	if strings.Count(logs.String(), redacted) != 2 || strings.Contains(logs.String(), secretToken) {
		t.Fatal("logger exposed credentials")
	}
}

func verifyJWT(t *testing.T, jwt string, key *rsa.PublicKey, now time.Time) {
	t.Helper()

	parts := strings.Split(jwt, ".")
	if len(parts) != 3 {
		t.Error("expected a three-part JWT")

		return
	}

	encoding := base64.RawURLEncoding
	sig, err := encoding.DecodeString(parts[2])
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	claims := fmt.Sprintf(`{"iss":%q,"iat":%d,"exp":%d}`, testAppID, now.Unix()-60, now.Unix()+540)

	if err != nil || parts[0] != encoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`)) ||
		parts[1] != encoding.EncodeToString([]byte(claims)) ||
		rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], sig) != nil {
		t.Error("JWT header, claims, or signature did not match")
	}
}

func TestKeyLoading(t *testing.T) {
	t.Parallel()

	der, err := x509.MarshalPKCS8PrivateKey(testKey(t))
	if err != nil {
		t.Fatal(err)
	}

	client := mustClient(t, Options{nil, nil, testAppID, writeKey(t, "PRIVATE KEY", der), "", 0})
	file := writeKey(t, "RSA PRIVATE KEY", []byte(secretKey))
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.pem")

	malformed := writeKey(t, "invalid\n"+secretKey, nil)

	for _, path := range []string{dir, missing, file, malformed, writeKey(t, "PRIVATE KEY", []byte(secretKey))} {
		_, err = NewClient(Options{nil, nil, "", path, "", 0})
		if err == nil || !strings.Contains(err.Error(), path) || strings.Contains(err.Error(), secretKey) ||
			errors.Is(err, fs.ErrNotExist) != (path == missing) {
			t.Fatal("expected a path-only key error with the correct cause")
		}
	}

	client.key = new(rsa.PrivateKey)

	_, err = client.Token(t.Context())
	if err == nil || !strings.Contains(err.Error(), "sign GitHub App JWT") {
		t.Fatal("expected a signing error")
	}
}

func TestExchangeErrors(t *testing.T) {
	t.Parallel()

	key := writeKey(t, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(testKey(t)))

	now := time.Unix(0, 0).UTC()
	client := mustClient(t, Options{nil, func() time.Time { return now }, testAppID, key, ":", 42})
	_, err := client.Token(t.Context())
	assertExchangeError(t, err, "create token request", secretKey)

	jwt, ok := client.sign(now)
	if !ok {
		t.Fatal("sign test JWT")
	}

	for _, test := range []struct {
		body   string
		status int
	}{
		{secretToken, http.StatusUnauthorized},
		{secretToken, http.StatusForbidden},
		{secretToken, http.StatusFound},
		{"invalid", http.StatusCreated},
		{`{"expires_at":"2026-01-01T01:00:00Z"}`, http.StatusCreated},
		{`{"token":"` + secretToken + `"}`, http.StatusCreated},
		{strings.Repeat(" ", responseLimit+1), http.StatusCreated},
		{"", http.StatusCreated},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
			if req.URL.Path != tokenPath {
				t.Error("token request followed a redirect")
			}

			writer.Header().Set("Location", "/other")

			if test.body == "" {
				writer.Header().Set("Content-Length", "1")
			}

			body := test.body
			if test.status != http.StatusCreated {
				body += req.Header.Get("Authorization")
			}

			writer.WriteHeader(test.status)
			//nolint:errcheck,gosec // The test server echoes a synthetic JWT and the client may close early.
			_, _ = io.WriteString(writer, body)
		}))
		client.options.BaseURL = server.URL
		_, err = client.Token(t.Context())

		server.Close()
		assertExchangeError(t, err, fmt.Sprintf("HTTP %d", test.status), jwt)
	}

	client.options.HTTPClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		//nolint:err113 // Deliberately simulate a transport error containing the Authorization header.
		return nil, errors.New(req.Header.Get("Authorization") + secretToken)
	})
	_, err = client.Token(t.Context())
	assertExchangeError(t, err, "send token request", "Bearer ")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func assertExchangeError(t *testing.T, err error, detail, secret string) {
	t.Helper()

	if err == nil || !strings.Contains(err.Error(), "POST "+tokenPath+": "+detail) ||
		strings.Contains(err.Error(), secretToken) || strings.Contains(err.Error(), secret) {
		t.Fatal("expected a redacted exchange error with method, path, and failure details")
	}
}

func mustClient(t *testing.T, options Options) *Client {
	t.Helper()

	client, err := NewClient(options)
	if err != nil {
		t.Fatal(err)
	}

	return client
}

func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	return key
}

func writeKey(t *testing.T, kind string, der []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "app.pem")
	data := pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der, Headers: nil})

	err := os.WriteFile(path, data, 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return path
}
