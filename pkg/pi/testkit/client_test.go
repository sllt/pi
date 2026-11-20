package testkit

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/auth"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/migration"
)

func TestClient_DirectVerbsImmutableDerivationsAndDecoding(t *testing.T) {
	app := New(t, pi.WithExplicitHTTPStatus())

	type itemReq struct {
		Name string `json:"name"`
	}
	type echoResp struct {
		Method    string   `json:"method"`
		Name      string   `json:"name,omitempty"`
		Header    string   `json:"header,omitempty"`
		Auth      string   `json:"auth,omitempty"`
		Subject   string   `json:"subject,omitempty"`
		Perms     []string `json:"perms,omitempty"`
		Session   string   `json:"session,omitempty"`
		QueryPage string   `json:"query_page,omitempty"`
	}

	app.GET("/echo", func(c *pi.Context) (any, error) {
		var sessionVal string
		if rawReq, ok := c.Request.(*piHTTP.Request); ok {
			if ck, err := rawReq.HTTPRequest().Cookie("sid"); err == nil {
				sessionVal = ck.Value
			}
		}
		p, _ := auth.FromContext(c)
		return echoResp{
			Method:    http.MethodGet,
			Header:    c.Header("X-Tenant"),
			Auth:      c.Header("Authorization"),
			Subject:   p.Subject,
			Perms:     p.Permissions,
			Session:   sessionVal,
			QueryPage: c.Param("page"),
		}, nil
	})

	app.POST("/items", func(c *pi.Context) (any, error) {
		var req itemReq
		if err := c.Bind(&req); err != nil {
			return nil, err
		}
		p, _ := auth.FromContext(c)
		return echoResp{
			Method:  http.MethodPost,
			Name:    req.Name,
			Header:  c.Header("X-Tenant"),
			Auth:    c.Header("Authorization"),
			Subject: p.Subject,
		}, nil
	})

	app.PUT("/items/{id}", func(c *pi.Context) (any, error) {
		var req itemReq
		if err := c.Bind(&req); err != nil {
			return nil, err
		}
		return echoResp{Method: http.MethodPut, Name: c.PathParam("id") + ":" + req.Name}, nil
	})

	app.PATCH("/items/{id}", func(c *pi.Context) (any, error) {
		var req itemReq
		if err := c.Bind(&req); err != nil {
			return nil, err
		}
		return echoResp{Method: http.MethodPatch, Name: c.PathParam("id") + ":" + req.Name}, nil
	})

	app.DELETE("/items/{id}", func(c *pi.Context) (any, error) {
		return echoResp{Method: http.MethodDelete, Name: c.PathParam("id")}, nil
	})

	base := NewClient(t, app)
	authed := base.
		WithHeader("X-Tenant", "acme").
		WithBearer("tok-123").
		WithPrincipal(auth.Principal{Subject: "user-1", Permissions: []string{"items:write"}}).
		WithCookie(&http.Cookie{Name: "sid", Value: "sess-42"})

	// Base client must remain unmutated.
	var baseEcho echoResp
	base.Get("/echo").ExpectStatus(http.StatusOK).Data(&baseEcho)
	assert.Empty(t, baseEcho.Header)
	assert.Empty(t, baseEcho.Auth)
	assert.Empty(t, baseEcho.Subject)
	assert.Empty(t, baseEcho.Session)

	// Derived client sends all derived headers, bearer, principal, and cookie.
	var authedEcho echoResp
	env := authed.Get("/echo?page=3").ExpectStatus(http.StatusOK).Data(&authedEcho).Envelope()
	assert.Equal(t, 0, env.Code)
	assert.Equal(t, "ok", env.Message)
	assert.Equal(t, "acme", authedEcho.Header)
	assert.Equal(t, "Bearer tok-123", authedEcho.Auth)
	assert.Equal(t, "user-1", authedEcho.Subject)
	assert.Equal(t, []string{"items:write"}, authedEcho.Perms)
	assert.Equal(t, "sess-42", authedEcho.Session)
	assert.Equal(t, "3", authedEcho.QueryPage)

	// One-line JSON & Delete verbs.
	var created echoResp
	authed.PostJSON("/items", itemReq{Name: "book"}).ExpectStatus(http.StatusOK).Data(&created)
	assert.Equal(t, http.MethodPost, created.Method)
	assert.Equal(t, "book", created.Name)
	assert.Equal(t, "user-1", created.Subject)

	var updated echoResp
	authed.PutJSON("/items/7", itemReq{Name: "pen"}).ExpectStatus(http.StatusOK).Data(&updated)
	assert.Equal(t, "7:pen", updated.Name)

	var patched echoResp
	authed.PatchJSON("/items/7", itemReq{Name: "ink"}).ExpectStatus(http.StatusOK).Data(&patched)
	assert.Equal(t, "7:ink", patched.Name)

	var deleted echoResp
	authed.Delete("/items/7").ExpectStatus(http.StatusOK).Data(&deleted)
	assert.Equal(t, http.MethodDelete, deleted.Method)
	assert.Equal(t, "7", deleted.Name)

	// Raw JSON(&v) decodes full envelope.
	var rawEnv map[string]any
	authed.Get("/echo").ExpectStatus(http.StatusOK).JSON(&rawEnv)
	assert.Equal(t, float64(0), rawEnv["code"])
	assert.Equal(t, "ok", rawEnv["message"])
}

func TestClient_RequestBuilderFormRawBodyAndQuery(t *testing.T) {
	app := New(t, pi.WithExplicitHTTPStatus())

	type formReq struct {
		Title string `form:"title"`
	}
	app.POST("/forms", func(c *pi.Context) (any, error) {
		var req formReq
		if err := c.Bind(&req); err != nil {
			return nil, err
		}
		var cookieVal string
		if rawReq, ok := c.Request.(*piHTTP.Request); ok {
			if ck, err := rawReq.HTTPRequest().Cookie("csrf"); err == nil {
				cookieVal = ck.Value
			}
		}
		p, _ := auth.FromContext(c)
		return map[string]any{
			"title":   req.Title,
			"csrf":    cookieVal,
			"mode":    c.Param("mode"),
			"subject": p.Subject,
			"trace":   c.Header("X-Trace"),
			"auth":    c.Header("Authorization"),
		}, nil
	})

	app.PUT("/raw", func(c *pi.Context) (any, error) {
		rawReq, ok := c.Request.(*piHTTP.Request)
		require.True(t, ok)
		body, err := rawReq.RawBodyBytes()
		require.NoError(t, err)
		return map[string]string{
			"ct":   c.Header("Content-Type"),
			"body": string(body),
		}, nil
	})

	c := NewClient(t, app)

	var formOut map[string]any
	c.R().
		Header("X-Trace", "tr-1").
		Headers(map[string]string{"X-Extra": "yes"}).
		Bearer("req-tok").
		Principal(auth.Principal{Subject: "form-user"}).
		Cookie(&http.Cookie{Name: "csrf", Value: "c-999"}).
		Query("mode", "preview").
		Form(url.Values{"title": {"hello-pi"}}).
		Post("/forms").
		ExpectStatus(http.StatusOK).
		Data(&formOut)

	assert.Equal(t, "hello-pi", formOut["title"])
	assert.Equal(t, "c-999", formOut["csrf"])
	assert.Equal(t, "preview", formOut["mode"])
	assert.Equal(t, "form-user", formOut["subject"])
	assert.Equal(t, "tr-1", formOut["trace"])
	assert.Equal(t, "Bearer req-tok", formOut["auth"])

	var rawOut map[string]string
	c.R().
		Body(strings.NewReader("plain-payload"), "text/plain").
		Put("/raw").
		ExpectStatus(http.StatusOK).
		Data(&rawOut)

	assert.Equal(t, "text/plain", rawOut["ct"])
	assert.Equal(t, "plain-payload", rawOut["body"])
}

type fallbackOnlyTB struct {
	t        *testing.T
	cleanups []func()
}

func (f *fallbackOnlyTB) Helper()                   { f.t.Helper() }
func (f *fallbackOnlyTB) Cleanup(fn func())         { f.cleanups = append(f.cleanups, fn) }
func (f *fallbackOnlyTB) Fatalf(s string, a ...any) { f.t.Fatalf(s, a...) }
func (f *fallbackOnlyTB) Errorf(s string, a ...any) { f.t.Errorf(s, a...) }

func TestNewSQLite_WithMigrationsAndAppIntegration(t *testing.T) {
	migrations := map[int64]migration.Migrate{
		1: {
			Name: "create_notes",
			UP: func(d migration.Datasource) error {
				_, err := d.SQL.Exec(`CREATE TABLE notes (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT NOT NULL);`)
				return err
			},
		},
		2: {
			Name: "seed_note",
			UP: func(d migration.Datasource) error {
				_, err := d.SQL.Exec(`INSERT INTO notes (body) VALUES ('hello sqlite');`)
				return err
			},
		},
	}

	db := NewSQLite(t, migrations)
	require.NotNil(t, db)
	assert.Equal(t, "sqlite", db.Dialect())

	app := New(t, pi.WithExplicitHTTPStatus(), pi.WithSQL(db, pi.Borrowed))
	app.GET("/notes", func(c *pi.Context) (any, error) {
		var body string
		if err := c.SQL().QueryRowContext(c, `SELECT body FROM notes LIMIT 1`).Scan(&body); err != nil {
			return nil, err
		}
		return map[string]string{"body": body}, nil
	})

	var out map[string]string
	NewClient(t, app).Get("/notes").ExpectStatus(http.StatusOK).Data(&out)
	assert.Equal(t, "hello sqlite", out["body"])

	t.Run("fallback temp dir when TB does not implement TempDir", func(t *testing.T) {
		fb := &fallbackOnlyTB{t: t}
		fbDB := NewSQLite(fb)
		require.NotNil(t, fbDB)
		require.NoError(t, fbDB.PingContext(t.Context()))

		for i := len(fb.cleanups) - 1; i >= 0; i-- {
			fb.cleanups[i]()
		}
		assert.Error(t, fbDB.PingContext(t.Context()))
	})
}

type recordingTB struct {
	fatalMsgs []string
	errorMsgs []string
}

func (r *recordingTB) Helper()        {}
func (r *recordingTB) Cleanup(func()) {}
func (r *recordingTB) Fatalf(s string, a ...any) {
	r.fatalMsgs = append(r.fatalMsgs, fmt.Sprintf(s, a...))
}
func (r *recordingTB) Errorf(s string, a ...any) {
	r.errorMsgs = append(r.errorMsgs, fmt.Sprintf(s, a...))
}

func TestClient_ErrorEnvelopeAndPathAndNilSafety(t *testing.T) {
	t.Run("Data rejects error envelope when Code != 0 even with non-null data", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rec.WriteHeader(http.StatusBadRequest)
		_, _ = rec.WriteString(`{"code":400,"message":"bad input","data":{"field":"name"}}`)

		tb := &recordingTB{}
		resp := &Response{ResponseRecorder: rec, t: tb}

		var dst map[string]string
		resp.Data(&dst)
		require.Len(t, tb.fatalMsgs, 1)
		assert.Contains(t, tb.fatalMsgs[0], "code=400")
		assert.Contains(t, tb.fatalMsgs[0], "bad input")
		assert.Empty(t, dst)
	})

	t.Run("nil Response and nil TB do not panic", func(t *testing.T) {
		var nilResp *Response
		require.NotPanics(t, func() {
			nilResp.ExpectStatus(http.StatusOK)
			nilResp.JSON(&struct{}{})
			nilResp.Data(&struct{}{})
			_ = nilResp.Envelope()
		})

		noTBResp := &Response{ResponseRecorder: httptest.NewRecorder()}
		noTBResp.WriteHeader(http.StatusCreated)
		require.NotPanics(t, func() {
			noTBResp.ExpectStatus(http.StatusOK)
			noTBResp.JSON(&struct{}{})
			noTBResp.Data(&struct{}{})
		})
	})

	t.Run("relative and empty paths normalize cleanly", func(t *testing.T) {
		app := New(t, pi.WithExplicitHTTPStatus())
		app.GET("/", func(c *pi.Context) (any, error) {
			return map[string]string{"path": "root"}, nil
		})
		app.GET("/ping", func(c *pi.Context) (any, error) {
			return map[string]string{"path": "ping"}, nil
		})

		c := NewClient(t, app)
		var out map[string]string
		c.Get("").ExpectStatus(http.StatusOK).Data(&out)
		assert.Equal(t, "root", out["path"])

		c.Get("ping").ExpectStatus(http.StatusOK).Data(&out)
		assert.Equal(t, "ping", out["path"])
	})
}
