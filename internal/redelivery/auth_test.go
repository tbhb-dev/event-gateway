// SPDX-License-Identifier: Apache-2.0
// Copyright Tony Burns

// cspell:ignore testpackage

package redelivery //nolint:testpackage // Exercise signer and request setup failures through private state.

import (
	"bytes"
	"crypto"
	"crypto/ed25519"
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
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/iotest"
	"time"
)

const (
	tokenPath   = "/app/installations/42/access_tokens" //nolint:gosec // This is a fake API path.
	secretToken = "test-installation-secret"
	secretKey   = "test-key-material"
	testAppID   = "12345"
	testOrg     = "test-org"
)

func TestTokenCacheAndJWT(t *testing.T) {
	t.Parallel()

	key := testKey(t)
	start := time.Unix(0, 0).UTC()
	now := start

	var calls atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.RequestURI() != tokenPath || req.ContentLength != 0 ||
			req.Header.Get("Accept") != "application/vnd.github+json" || req.UserAgent() != "event-gateway" ||
			req.Header.Get("X-Github-Api-Version") != "2022-11-28" {
			t.Error("unexpected installation token request")
		}

		call := calls.Add(1)
		if call == 1 {
			verifyJWT(t, req.Header.Get("Authorization"), &key.PublicKey, start)
		}

		writer.WriteHeader(http.StatusCreated)
		//nolint:errcheck // The client reports an incomplete response.
		_, _ = io.Copy(writer, tokenResponse(fmt.Sprint(secretToken, call), start.Add(time.Duration(call)*time.Hour)))
	}))
	t.Cleanup(server.Close)

	file := keyFile(t, keyPEM("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key)))
	clock := func() time.Time { return now }
	client, err := NewClient(Options{
		HTTPClient: server.Client(), Now: clock, AppID: testAppID, KeyFile: file, BaseURL: server.URL + "/",
		InstallationID: 42, Org: testOrg, HookID: 123,
	})
	noError(t, err)

	check := func(want int32) {
		t.Helper()

		token, tokenErr := client.Token(t.Context())
		if tokenErr != nil || token != fmt.Sprint(secretToken, want) || calls.Load() != want {
			t.Fatalf("token exchange %d failed: %v", want, tokenErr)
		}
	}
	check(1)
	check(1)

	now = start.Add(58 * time.Minute)

	check(2)

	client.Invalidate()
	check(3)

	for _, value := range []any{client, *client, client.cache.token} {
		if fmt.Sprintf("%v %+v %#v", value, value, value) != strings.Repeat(redacted+" ", 2)+redacted ||
			slog.AnyValue(value).Resolve().String() != redacted {
			t.Fatal("formatter or logger exposed credentials")
		}
	}

	checkConcurrentTokens(t, client)
}

func checkConcurrentTokens(t *testing.T, client *Client) {
	t.Helper()

	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			client.Invalidate()

			token, err := client.Token(t.Context())
			if token == "" || err != nil {
				t.Error("expected a token and no error")
			}
		})
	}

	group.Wait()
}

func verifyJWT(t *testing.T, auth string, key *rsa.PublicKey, now time.Time) {
	t.Helper()

	encoding := base64.RawURLEncoding
	claims := fmt.Sprintf(`{"iss":%q,"iat":%d,"exp":%d}`, testAppID, now.Unix()-60, now.Unix()+540)
	header := encoding.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	unsigned := header + "." + encoding.EncodeToString([]byte(claims))
	sig, ok := strings.CutPrefix(auth, "Bearer "+unsigned+".")
	decoded, err := encoding.DecodeString(sig)
	digest := sha256.Sum256([]byte(unsigned))

	if !ok || err != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, digest[:], decoded) != nil {
		t.Error("JWT header, claims, or signature did not match")
	}
}

func TestKeyErrors(t *testing.T) {
	t.Parallel()

	der := pkcs8(t, testKey(t))
	key := keyPEM("PRIVATE KEY", der)
	nonRSA := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	encrypted := &pem.Block{Type: "PRIVATE KEY", Bytes: der, Headers: map[string]string{"Proc-Type": "4,ENCRYPTED"}}

	for name, data := range map[string][]byte{
		"invalid RSA":       keyPEM("RSA PRIVATE KEY", []byte(secretKey)),
		"invalid PKCS8":     keyPEM("PRIVATE KEY", []byte(secretKey)),
		"non-RSA":           keyPEM("PRIVATE KEY", pkcs8(t, nonRSA)),
		"encrypted header":  pem.EncodeToMemory(encrypted),
		"extra key":         bytes.Repeat(key, 2),
		"extra certificate": append(bytes.Clone(key), keyPEM("CERTIFICATE", der)...),
		"trailing data":     append(bytes.Clone(key), secretKey...),
		"wrong type":        keyPEM("PUBLIC KEY", der),
		"malformed":         []byte("invalid\n" + secretKey),
		"empty":             nil,
	} {
		path := keyFile(t, data)

		_, err := NewClient(Options{nil, nil, "", path, "", testOrg, 0, 123})
		if err == nil || err.Error() != "parse GitHub App RSA key "+path {
			t.Errorf("%s: expected a path-only parse error", name)
		}
	}

	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.pem")

	for _, path := range []string{dir, missing} {
		_, err := NewClient(Options{nil, nil, "", path, "", testOrg, 0, 123})
		if err == nil || !strings.Contains(err.Error(), path) || errors.Is(err, fs.ErrNotExist) != (path == missing) {
			t.Fatal("expected a path-only read error with the correct cause")
		}
	}
}

func TestExchangeErrors(t *testing.T) {
	t.Parallel()

	now := time.Unix(0, 0).UTC()
	key := keyPEM("PRIVATE KEY", pkcs8(t, testKey(t)))
	client, err := NewClient(
		Options{nil, nil, testAppID, keyFile(t, append(key, " \t\n"...)), ":", testOrg, 42, 123},
	)
	noError(t, err)

	client.options.Now = func() time.Time { return now }
	assertTokenError(t, client, "invalid URL", "create token request")

	client.options.BaseURL = "https://example.test"

	badExpiry := `{"token":"` + secretToken + `","expires_at":"` + secretKey + `"}`
	for _, test := range []struct {
		body   io.Reader
		name   string
		status int
	}{
		{strings.NewReader(secretToken), "unauthorized", http.StatusUnauthorized},
		{strings.NewReader(secretToken), "forbidden", http.StatusForbidden},
		{strings.NewReader(secretToken), "redirect", http.StatusFound},
		{strings.NewReader(secretKey), "invalid JSON", http.StatusCreated},
		{tokenResponse("", now.Add(time.Hour)), "missing token", http.StatusCreated},
		{strings.NewReader(`{"token":"` + secretToken + `"}`), "missing expiry", http.StatusCreated},
		{strings.NewReader(badExpiry), "unparsable expiry", http.StatusCreated},
		{tokenResponse(secretToken, now.Add(-time.Hour)), "expired", http.StatusCreated},
		{tokenResponse(secretToken, now.Add(time.Minute)), "inside margin", http.StatusCreated},
		{tokenResponse(secretToken, now.Add(2*time.Minute)), "at margin", http.StatusCreated},
		{strings.NewReader(strings.Repeat(" ", responseLimit+1)), "oversized", http.StatusCreated},
		{iotest.ErrReader(io.ErrUnexpectedEOF), "read error", http.StatusCreated},
	} {
		client.options.HTTPClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != tokenPath {
				t.Error("token request followed a redirect")
			}

			//nolint:exhaustruct_v5 // The transport supplies only fields used by the client.
			return &http.Response{
				StatusCode: test.status, Header: http.Header{"Location": {"/other"}}, Body: io.NopCloser(test.body),
			}, nil
		})

		assertTokenError(t, client, test.name, fmt.Sprintf("HTTP %d", test.status))
	}

	client.options.HTTPClient.Transport = roundTripFunc(func(req *http.Request) (*http.Response, error) {
		//nolint:err113 // Simulate a transport error containing the Authorization header.
		return nil, errors.New(req.Header.Get("Authorization") + secretToken)
	})
	assertTokenError(t, client, "transport", "send token request")
	client.key = new(rsa.PrivateKey)
	assertTokenError(t, client, "sign", "sign GitHub App JWT")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func tokenResponse(token string, expiry time.Time) *strings.Reader {
	return strings.NewReader(fmt.Sprintf(`{"token":%q,"expires_at":%q}`, token, expiry.Format(time.RFC3339)))
}

func assertTokenError(t *testing.T, client *Client, name, detail string) {
	t.Helper()

	var empty installationToken

	token, err := client.Token(t.Context())
	if token != "" || client.cache.token != empty || err == nil ||
		strings.TrimSuffix(err.Error(), " invalid token response") != "POST "+tokenPath+": "+detail {
		t.Errorf("%s: expected a redacted exchange error without a returned or cached token", name)
	}
}

func pkcs8(t *testing.T, key any) []byte {
	t.Helper()

	der, err := x509.MarshalPKCS8PrivateKey(key)
	noError(t, err)

	return der
}

func testKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	noError(t, err)

	return key
}

func keyPEM(kind string, der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der, Headers: nil})
}

func keyFile(t *testing.T, data []byte) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "app.pem")
	noError(t, os.WriteFile(path, data, 0o600))

	return path
}

func noError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal(err)
	}
}

const (
	deliveriesPath = "/orgs/Test-org9/hooks/789/deliveries"
	plain          = "plain"
	authKind       = "auth"
	transientKind  = "transient"
	privateMarker  = "private-marker"
)

func deliveryOptions(t *testing.T) Options {
	t.Helper()

	return Options{
		HTTPClient: nil, Now: func() time.Time { return time.Unix(1000, 0) },
		AppID: testAppID, KeyFile: keyFile(t, keyPEM("PRIVATE KEY", pkcs8(t, testKey(t)))),
		BaseURL: "", Org: "Test-org9", InstallationID: 42, HookID: 789,
	}
}

func TestDeliveryOptions(t *testing.T) {
	t.Parallel()

	options := deliveryOptions(t)
	for org, id := range map[string]int64{"": 789, "../other": 789, "other%2F": 789, options.Org: 0, "org": -1} {
		options.Org, options.HookID = org, id

		_, err := NewClient(options)
		if err == nil {
			t.Fatalf("accepted invalid webhook %q/%d", org, id)
		}
	}
}

func TestListDeliveries(t *testing.T) {
	t.Parallel()

	const (
		page = `[{"id":9,"guid":"test-guid","delivered_at":"2026-01-01T00:00:00Z","redelivery":true,
"status":"failed","status_code":500,"event":"push","action":"ignored"},{"id":4}]`
		next = "a+b&c"
	)

	handle := func(writer http.ResponseWriter, req *http.Request, _ int32) {
		if req.Method != http.MethodGet || req.URL.Path != deliveriesPath || req.URL.Query().Get("per_page") != "100" {
			t.Error("unexpected list request")
		}

		switch req.URL.Query().Get("cursor") {
		case "":
			writer.Header().Set("Link", `<https://example.test/?cursor=unused>; rel="last"`)
			writer.Header().Add("Link", `<https://example.test/other?cursor=a%2Bb%26c>; rel="next"`)
		case "bad-link":
			writer.Header().Set("Link", `<%>; rel="next"`)
		case next:
		default:
			t.Error("unexpected list cursor")
		}

		writeResponse(writer, http.StatusOK, page)
	}
	client, exchanges := deliveryClient(t, deliveryOptions(t), handle)

	for _, tc := range [][2]string{{"", next}, {next, ""}, {"bad-link", ""}} {
		got, cursor, err := client.ListDeliveries(t.Context(), tc[0])
		noError(t, err)

		want := Delivery{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "test-guid", "failed", 9, 500, true}
		if len(got) != 2 || got[0] != want || got[1].ID != 4 || cursor != tc[1] || exchanges.Load() != 1 {
			t.Fatalf("unexpected delivery page or cursor %q", cursor)
		}
	}
}

type deliveryCase struct {
	header                   http.Header
	name, body, kind, detail string
	status                   int
	exchanges                int32
	list                     bool
}

func TestDeliveryErrors(t *testing.T) {
	t.Parallel()

	const (
		echo        = `{"message":"` + privateMarker + " Bearer " + secretToken + `1"}`
		badResponse = " invalid delivery response"
		remaining   = "X-Ratelimit-Remaining"
		reset       = "X-Ratelimit-Reset"
		retry       = "Retry-After"
	)

	message := strings.Repeat("é", 100)
	longMessage := `{"message":"\n` + message + privateMarker + `"}`
	options := deliveryOptions(t)

	for _, test := range []deliveryCase{
		{nil, "accepted", "", "", "", 202, 1, false},
		{nil, "wrong success", "", plain, "", 200, 1, false},
		{nil, "retry succeeds", "[]", "", "", 200, 2, true},
		{nil, "retry rejected", echo, authKind, "", 401, 2, false},
		{http.Header{retry: {"15"}, reset: {"1002"}}, "retry after", "", "15s", "", 429, 1, false},
		{http.Header{remaining: {"0"}, retry: {"invalid"}, reset: {"1075"}}, "reset", "", "1m15s", "", 403, 1, false},
		{http.Header{retry: {"9000"}}, "cap", "", "1h0m0s", "", 403, 1, false},
		{http.Header{remaining: {"0"}, reset: {"999"}}, "past", "", "0s", "", 429, 1, false},
		{http.Header{remaining: {"0"}}, "missing reset", "", "0s", "", 429, 1, false},
		{nil, "forbidden", "", authKind, "", 403, 1, false},
		{nil, "server failure", longMessage, transientKind, ": " + message, 500, 1, true},
		{nil, "empty message", `{"other":"` + privateMarker + `"}`, plain, "", 600, 1, false},
		{nil, "bad JSON", privateMarker, plain, badResponse, 200, 1, true},
		{nil, "over limit", "[]" + strings.Repeat(" ", responseLimit), plain, badResponse, 200, 1, true},
		{http.Header{"Content-Length": {"10"}}, "truncated body", "[]", transientKind, " read response", 200, 1, true},
		{http.Header{"Location": {"/elsewhere"}}, "redirect", "", plain, "", 307, 1, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			test.run(t, options)
		})
	}
}

func (tc deliveryCase) run(t *testing.T, options Options) {
	t.Helper()

	path, method := deliveriesPath+"/97/attempts", http.MethodPost
	if tc.list {
		path, method = deliveriesPath, http.MethodGet
	}

	var calls atomic.Int32

	handle := func(writer http.ResponseWriter, req *http.Request, exchange int32) {
		if req.Method != method || req.URL.Path != path {
			t.Error("unexpected delivery endpoint")
		}

		calls.Add(1)

		if exchange < tc.exchanges {
			writer.WriteHeader(http.StatusUnauthorized)

			return
		}

		maps.Copy(writer.Header(), tc.header)
		writeResponse(writer, tc.status, tc.body)
	}
	client, exchanges := deliveryClient(t, options, handle)

	var err error
	if tc.list {
		_, _, err = client.ListDeliveries(t.Context(), "")
	} else {
		err = client.Redeliver(t.Context(), 97)
	}

	if exchanges.Load() != tc.exchanges || calls.Load() != tc.exchanges {
		t.Fatal("unexpected token exchange or delivery call count")
	}

	checkDeliveryError(t, err, tc.kind, fmt.Sprintf("%s %s: HTTP %d%s", method, path, tc.status, tc.detail))
}

func TestDeliveryRequestFailure(t *testing.T) {
	t.Parallel()

	options := deliveryOptions(t)
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected request")
	}))
	server.Close()
	options.BaseURL = server.URL
	client, err := NewClient(options)
	noError(t, err)

	checkDeliveryError(t, client.Redeliver(t.Context(), 1), authKind, "POST "+tokenPath+": send token request")
	client.cache.token = installationToken{options.Now().Add(time.Hour), secretToken}

	const request = "POST " + deliveriesPath + "/1/attempts: "
	checkDeliveryError(t, client.Redeliver(t.Context(), 1), transientKind, request+"send request")
	client.options.BaseURL = ":"
	checkDeliveryError(t, client.Redeliver(t.Context(), 1), plain, request+"create request")
}

func checkDeliveryError(t *testing.T, err error, kind, want string) {
	t.Helper()

	if kind == "" {
		noError(t, err)

		return
	}

	got := plain

	switch {
	case errors.As(err, new(*AuthError)):
		got = authKind
	case errors.As(err, new(*TransientError)):
		got = transientKind
	default:
		if rate, ok := errors.AsType[*RateLimitError](err); ok {
			got = rate.Wait.String()
		}
	}

	if got != kind || err == nil || err.Error() != want {
		t.Fatalf("expected sanitized %s error", kind)
	}

	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).ErrorContext(t.Context(), "delivery", "error", err)

	for _, value := range []string{privateMarker, secretToken, "Bearer ", "Authorization"} {
		if strings.Contains(err.Error(), value) || strings.Contains(logs.String(), value) {
			t.Fatal("error exposed private data")
		}
	}
}

func deliveryClient(
	t *testing.T, options Options, handle func(http.ResponseWriter, *http.Request, int32),
) (*Client, *atomic.Int32) {
	t.Helper()

	var exchanges atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, req *http.Request) {
		if req.ContentLength != 0 || req.Header.Get("Accept") != "application/vnd.github+json" ||
			req.Header.Get("X-Github-Api-Version") != "2022-11-28" || req.UserAgent() != "event-gateway" {
			t.Error("unexpected request body or headers")
		}

		if req.URL.RequestURI() == tokenPath && req.Method == http.MethodPost {
			token := fmt.Sprint(secretToken, exchanges.Add(1))

			writer.WriteHeader(http.StatusCreated)
			//nolint:errcheck // The client reports an incomplete token response.
			_, _ = io.Copy(writer, tokenResponse(token, options.Now().Add(time.Hour)))

			return
		}

		exchange := exchanges.Load()
		if req.Header.Get("Authorization") != fmt.Sprint("Bearer ", secretToken, exchange) {
			t.Error("unexpected installation authorization")
		}

		handle(writer, req, exchange)
	}))
	t.Cleanup(server.Close)
	options.BaseURL, options.HTTPClient = server.URL, server.Client()
	client, err := NewClient(options)
	noError(t, err)

	return client, &exchanges
}

func writeResponse(w http.ResponseWriter, status int, body string) {
	w.WriteHeader(status)
	//nolint:errcheck // The client test checks the response it receives.
	_, _ = io.WriteString(w, body)
}
