package pi_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/apperror"
	"github.com/sllt/pi/pkg/pi/openapi"
	"github.com/sllt/pi/pkg/pi/testkit"
	"github.com/stretchr/testify/require"
)

type apiEnvelope struct {
	Code    int             `json:"code"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
	Details []struct {
		Field string `json:"field"`
		Rule  string `json:"rule"`
	} `json:"details"`
}

type registerPingReq struct {
	pi.Meta `path:"/ping/{id}" method:"GET" tags:"ping" summary:"Ping"`
	ID      string `path:"id" validate:"required"`
	Tag     string `query:"tag"`
}

type registerPingRes struct {
	ID  string `json:"id"`
	Tag string `json:"tag,omitempty"`
}

type groupUserGetReq struct {
	pi.Meta `path:"" method:"GET" summary:"Get user root"`
}

type groupUserPutReq struct {
	pi.Meta `path:"/" method:"PUT" status:"204" summary:"Update user root"`
}

type groupUserRes struct {
	Name string `json:"name"`
}

type emptyRes struct{}

type typedBindCtrl struct{}

func (c *typedBindCtrl) Get(ctx *pi.Context, req *groupUserGetReq) (*groupUserRes, error) {
	return &groupUserRes{Name: "from-ctrl"}, nil
}

type createItemReq struct {
	pi.Meta `path:"/items" method:"POST" status:"201" tags:"items" summary:"Create item"`
	Name    string `json:"name" validate:"required,min=2"`
}

type createItemRes struct {
	ID string `json:"id"`
}

type badInvalidValidateReq struct {
	pi.Meta `path:"/broken" method:"POST"`
	Name    string `json:"name" validate:"not_a_real_rule"`
}

type mixedLegacyCtrl struct{}

func (c *mixedLegacyCtrl) Typed(ctx *pi.Context, req *createItemReq) (*createItemRes, error) {
	return &createItemRes{ID: "1"}, nil
}

func (c *mixedLegacyCtrl) Legacy(ctx *pi.Context) (any, error) {
	return "ok", nil
}

type multiMethodBadSecondCtrl struct{}

func (c *multiMethodBadSecondCtrl) AFirst(ctx *pi.Context, req *createItemReq) (*createItemRes, error) {
	return &createItemRes{ID: "first"}, nil
}

func (c *multiMethodBadSecondCtrl) BSecond(ctx *pi.Context, req *badInvalidValidateReq) (*createItemRes, error) {
	return &createItemRes{ID: "second"}, nil
}

func TestTypedRegisterBindAndErrorLedger(t *testing.T) {
	t.Run("register on app and group plus bind methods", func(t *testing.T) {
		app := testkit.New(t,
			pi.WithExplicitHTTPStatus(),
			pi.WithOpenAPI(pi.OpenAPIConfig{Title: "TypedAPI", Version: "1.0.0"}),
		)

		err := pi.Register(app, func(ctx *pi.Context, req *registerPingReq) (*registerPingRes, error) {
			return &registerPingRes{ID: req.ID, Tag: req.Tag}, nil
		})
		require.NoError(t, err)

		v1 := app.Group("/api/v1")
		ctrl := &typedBindCtrl{}
		require.NoError(t, v1.Group("/user").Bind(ctrl))

		createHandler := func(ctx *pi.Context, req *createItemReq) (*createItemRes, error) {
			return &createItemRes{ID: "item-" + req.Name}, nil
		}
		updateHandler := func(ctx *pi.Context, req *groupUserPutReq) (*emptyRes, error) {
			return nil, nil
		}
		require.NoError(t, v1.Bind(createHandler, updateHandler))
		require.NoError(t, app.RouteError())

		r1 := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/ping/42?tag=fast", nil))
		require.Equal(t, http.StatusOK, r1.Code)
		var env1 apiEnvelope
		require.NoError(t, json.Unmarshal(r1.Body.Bytes(), &env1))
		require.Contains(t, string(env1.Data), `"id":"42"`)
		require.Contains(t, string(env1.Data), `"tag":"fast"`)

		r2 := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/api/v1/user", nil))
		require.Equal(t, http.StatusOK, r2.Code)
		require.Contains(t, r2.Body.String(), `"from-ctrl"`)

		postReq := httptest.NewRequest(http.MethodPost, "/api/v1/items", strings.NewReader(`{"name":"book"}`))
		postReq.Header.Set("Content-Type", "application/json")
		r3 := testkit.Request(t, app, postReq)
		require.Equal(t, http.StatusCreated, r3.Code)
		require.Contains(t, r3.Body.String(), `"item-book"`)
	})

	t.Run("bind legacy hint and atomic rollback", func(t *testing.T) {
		app := testkit.New(t)
		err := app.Group("/api").Bind(&mixedLegacyCtrl{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "legacy signature")
		require.Contains(t, err.Error(), "pi.Register")
		require.Error(t, app.RouteError())

		// Atomic rollback: when BSecond fails, AFirst must NOT be committed either.
		atomicApp := testkit.New(t)
		v1 := atomicApp.Group("/api/v1")
		err = v1.Bind(&multiMethodBadSecondCtrl{})
		require.Error(t, err)

		// Re-registering AFirst's route later must not trigger a duplicate route conflict.
		reRegErr := pi.Register(v1, func(ctx *pi.Context, req *createItemReq) (*createItemRes, error) {
			return &createItemRes{ID: "ok"}, nil
		})
		require.NoError(t, reRegErr)
		require.NotContains(t, atomicApp.RouteError().Error(), "route conflict")

		// Bad target shapes rejected with clear errors.
		nilApp := testkit.New(t)
		require.Error(t, nilApp.Bind())
		require.Error(t, nilApp.Bind(struct{}{}))
		require.Error(t, nilApp.Bind((*typedBindCtrl)(nil)))
		var nilFn func(*pi.Context, *createItemReq) (*createItemRes, error)
		require.Error(t, nilApp.Bind(nilFn))
	})

	t.Run("ignored register error blocks RouteError HTTPHandler OpenAPISpec and Start even with HTTP_ENABLED=false", func(t *testing.T) {
		for _, httpEnabled := range []string{"true", "false"} {
			app := testkit.New(t,
				pi.WithConfig(map[string]string{
					"HTTP_ENABLED":    httpEnabled,
					"HTTP_PORT":       "0",
					"GRPC_ENABLED":    "false",
					"METRICS_ENABLED": "false",
					"PI_TELEMETRY":    "false",
					"LOG_LEVEL":       "ERROR",
				}),
				pi.WithOpenAPI(pi.OpenAPIConfig{Title: "ErrApp", Version: "1.0.0"}),
			)

			// Intentionally ignore return value from pi.Register.
			_ = pi.Register(app, func(ctx *pi.Context, req *badInvalidValidateReq) (*createItemRes, error) {
				return &createItemRes{}, nil
			})

			require.Error(t, app.RouteError())

			h, err := app.HTTPHandler()
			require.Error(t, err)
			require.Nil(t, h)

			spec, err := app.OpenAPISpec()
			require.Error(t, err)
			require.Nil(t, spec)

			startErr := app.Start(t.Context())
			require.Error(t, startErr)
		}
	})

	t.Run("frozen mutations return ErrRoutesFrozen and preserve compiled spec", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "FreezeAPI", Version: "1.0.0"}))
		grp := app.Group("/api").WithDoc(pi.DocTags("initial"))
		require.NoError(t, pi.Register(grp, func(ctx *pi.Context, req *createItemReq) (*createItemRes, error) {
			return &createItemRes{ID: "1"}, nil
		}))

		_, err := app.HTTPHandler()
		require.NoError(t, err)

		// Post-freeze Register and Bind return ErrRoutesFrozen without corrupting RouteError/spec.
		err = pi.Register(app, func(ctx *pi.Context, req *registerPingReq) (*registerPingRes, error) {
			return &registerPingRes{ID: req.ID}, nil
		})
		require.ErrorIs(t, err, pi.ErrRoutesFrozen)
		require.ErrorIs(t, app.Bind(&typedBindCtrl{}), pi.ErrRoutesFrozen)
		require.ErrorIs(t, grp.Bind(&typedBindCtrl{}), pi.ErrRoutesFrozen)

		// Post-freeze WithDoc and Use are ignored cleanly.
		grp.WithDoc(pi.DocTags("mutated-after-freeze"))
		app.Use(func(next http.Handler) http.Handler { return next })
		require.NoError(t, app.RouteError())

		spec, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Contains(t, spec.Paths, "/api/items")
		require.Equal(t, []string{"items"}, spec.Paths["/api/items"].Post.Tags)
	})
}

func TestTypedGroupRootAndConflicts(t *testing.T) {
	t.Run("group root canonical path without trailing slash serves both with and without slash", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "GroupRoot", Version: "1.0.0"}))
		userGroup := app.Group("/api/v1").Group("/user")

		require.NoError(t, pi.Register(userGroup, func(ctx *pi.Context, req *groupUserGetReq) (*groupUserRes, error) {
			return &groupUserRes{Name: "alice"}, nil
		}))
		require.NoError(t, pi.Register(userGroup, func(ctx *pi.Context, req *groupUserPutReq) (*emptyRes, error) {
			return nil, nil
		}))

		spec, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Contains(t, spec.Paths, "/api/v1/user")
		require.NotContains(t, spec.Paths, "/api/v1/user/")
		require.NotNil(t, spec.Paths["/api/v1/user"].Get)
		require.NotNil(t, spec.Paths["/api/v1/user"].Put)

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		for _, targetURL := range []string{"/api/v1/user", "/api/v1/user/"} {
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, targetURL, nil))
			require.Equal(t, http.StatusOK, rec.Code, "target=%s", targetURL)
			require.Contains(t, rec.Body.String(), `"alice"`)

			putRec := httptest.NewRecorder()
			h.ServeHTTP(putRec, httptest.NewRequest(http.MethodPut, targetURL, nil))
			require.Equal(t, http.StatusNoContent, putRec.Code, "target=%s", targetURL)
		}
	})

	t.Run("parent and child group conflict intercepted before chi.Mount panic", func(t *testing.T) {
		app := testkit.New(t)
		apiV1 := app.Group("/api/v1")
		apiV1.GET("/user", func(c *pi.Context) (any, error) { return "legacy", nil })
		userGroup := apiV1.Group("/user")
		err := pi.Register(userGroup, func(ctx *pi.Context, req *groupUserGetReq) (*groupUserRes, error) {
			return &groupUserRes{Name: "typed"}, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "conflict")
		require.NotPanics(t, func() {
			_, compileErr := app.HTTPHandler()
			require.Error(t, compileErr)
		})
	})

	t.Run("typed vs legacy and system probe conflicts plus duplicate operationId", func(t *testing.T) {
		// Typed registered first, legacy registered later -> HTTPHandler catches conflict.
		app1 := testkit.New(t)
		require.NoError(t, pi.Register(app1, func(ctx *pi.Context, req *registerPingReq) (*registerPingRes, error) {
			return &registerPingRes{ID: req.ID}, nil
		}))
		app1.GET("/ping/{itemId}", func(c *pi.Context) (any, error) { return "legacy", nil })
		require.Error(t, app1.RouteError())
		_, err := app1.HTTPHandler()
		require.Error(t, err)

		// Typed colliding with built-in system probe route.
		type probeConflictReq struct {
			pi.Meta `path:"/.well-known/health" method:"GET"`
		}
		app2 := testkit.New(t)
		err = pi.Register(app2, func(ctx *pi.Context, req *probeConflictReq) (*emptyRes, error) {
			return nil, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "system route")

		// Duplicate operationId rejected.
		type dupOpReq1 struct {
			pi.Meta `path:"/one" method:"GET" operationId:"sharedOp"`
		}
		type dupOpReq2 struct {
			pi.Meta `path:"/two" method:"GET" operationId:"sharedOp"`
		}
		app3 := testkit.New(t)
		require.NoError(t, pi.Register(app3, func(ctx *pi.Context, req *dupOpReq1) (*emptyRes, error) { return nil, nil }))
		err = pi.Register(app3, func(ctx *pi.Context, req *dupOpReq2) (*emptyRes, error) { return nil, nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "operationId")
	})
}

func TestTypedOptionsStatusesAndRuntime(t *testing.T) {
	t.Run("WithDoc before and after Register plus overrides and conflicts", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:   "DocsAPI",
			Version: "1.0.0",
			SecuritySchemes: map[string]openapi.SecurityScheme{
				"Bearer": {Type: "http", Scheme: "bearer"},
				"ApiKey": {Type: "apiKey", Name: "X-API-Key", In: "header"},
			},
		}))

		type opBeforeReq struct {
			pi.Meta `path:"/before" method:"GET"`
		}
		type opAfterReq struct {
			pi.Meta `path:"/after" method:"GET"`
		}
		type opChildReq struct {
			pi.Meta `path:"/child" method:"GET"`
		}
		type opEndpointOverrideReq struct {
			pi.Meta `path:"/override" method:"GET"`
		}

		api := app.Group("/api")
		require.NoError(t, pi.Register(api, func(ctx *pi.Context, req *opBeforeReq) (*emptyRes, error) { return nil, nil }))
		// Applied AFTER Register: must still propagate to /api/before!
		api.WithDoc(pi.DocTags("core"), pi.DocSecurity("Bearer"))
		// Identical second call is idempotent.
		api.WithDoc(pi.DocTags("core"), pi.DocSecurity("Bearer"))
		require.NoError(t, api.Bind(func(ctx *pi.Context, req *opAfterReq) (*emptyRes, error) { return nil, nil }))

		child := api.Group("/public").WithDoc(pi.DocTags("public"), pi.DocNoSecurity())
		require.NoError(t, pi.Register(child, func(ctx *pi.Context, req *opChildReq) (*emptyRes, error) { return nil, nil }))
		require.NoError(t, pi.Register(api, func(ctx *pi.Context, req *opEndpointOverrideReq) (*emptyRes, error) { return nil, nil },
			pi.WithEndpointDoc(pi.DocTags("ep-tag"), pi.DocSecurity("ApiKey")),
		))

		spec, err := app.OpenAPISpec()
		require.NoError(t, err)

		for _, p := range []string{"/api/before", "/api/after"} {
			op := spec.Paths[p].Get
			require.Equal(t, []string{"core"}, op.Tags)
			require.NotNil(t, op.Security)
			require.Equal(t, []openapi.SecurityRequirement{{"Bearer": {}}}, *op.Security)
		}

		pubOp := spec.Paths["/api/public/child"].Get
		require.Equal(t, []string{"public"}, pubOp.Tags)
		require.NotNil(t, pubOp.Security)
		require.Empty(t, *pubOp.Security)

		epOp := spec.Paths["/api/override"].Get
		require.Equal(t, []string{"ep-tag"}, epOp.Tags)
		require.NotNil(t, epOp.Security)
		require.Equal(t, []openapi.SecurityRequirement{{"ApiKey": {}}}, *epOp.Security)

		// Conflicting WithDoc on the same group records error.
		api.WithDoc(pi.DocTags("different-tag"))
		require.Error(t, app.RouteError())
	})

	t.Run("endpoint middlewares order and endpoint timeout", func(t *testing.T) {
		app := testkit.New(t)
		var order []string
		grp := app.Group("/v1")
		grp.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, "grp-http")
				next.ServeHTTP(w, r)
			})
		})
		grp.UseMiddleware(func(next pi.Handler) pi.Handler {
			return func(c *pi.Context) (any, error) {
				order = append(order, "grp-pi")
				return next(c)
			}
		})

		type orderReq struct {
			pi.Meta `path:"/order" method:"GET"`
		}
		type orderRes struct {
			OK bool `json:"ok"`
		}
		require.NoError(t, pi.Register(grp, func(ctx *pi.Context, req *orderReq) (*orderRes, error) {
			order = append(order, "handler")
			return &orderRes{OK: true}, nil
		},
			pi.WithEndpointHTTPMiddleware(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					order = append(order, "ep-http")
					next.ServeHTTP(w, r)
				})
			}),
			pi.WithEndpointMiddleware(func(next pi.Handler) pi.Handler {
				return func(c *pi.Context) (any, error) {
					order = append(order, "ep-pi")
					return next(c)
				}
			}),
		))

		type slowReq struct {
			pi.Meta `path:"/slow" method:"GET"`
		}
		var hasDeadline atomic.Bool
		require.NoError(t, pi.Register(grp, func(ctx *pi.Context, req *slowReq) (*orderRes, error) {
			_, ok := ctx.Deadline()
			hasDeadline.Store(ok)
			<-ctx.Done()
			return nil, ctx.Err()
		}, pi.WithEndpointTimeout(20*time.Millisecond)))

		// Negative and nil endpoint option validation.
		require.Error(t, pi.Register(grp, func(ctx *pi.Context, req *orderReq) (*orderRes, error) { return &orderRes{}, nil }, pi.WithEndpointTimeout(-time.Second)))
		require.Error(t, pi.Register(grp, func(ctx *pi.Context, req *orderReq) (*orderRes, error) { return &orderRes{}, nil }, pi.WithEndpointMiddleware(nil)))
		require.Error(t, pi.Register(grp, func(ctx *pi.Context, req *orderReq) (*orderRes, error) { return &orderRes{}, nil }, pi.WithEndpointHTTPMiddleware(nil)))

		// Separate valid app for runtime HTTP check.
		rtApp := testkit.New(t)
		order = nil
		rtGrp := rtApp.Group("/v1")
		rtGrp.Use(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, "grp-http")
				next.ServeHTTP(w, r)
			})
		})
		rtGrp.UseMiddleware(func(next pi.Handler) pi.Handler {
			return func(c *pi.Context) (any, error) {
				order = append(order, "grp-pi")
				return next(c)
			}
		})
		require.NoError(t, pi.Register(rtGrp, func(ctx *pi.Context, req *orderReq) (*orderRes, error) {
			order = append(order, "handler")
			return &orderRes{OK: true}, nil
		},
			pi.WithEndpointHTTPMiddleware(func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					order = append(order, "ep-http")
					next.ServeHTTP(w, r)
				})
			}),
			pi.WithEndpointMiddleware(func(next pi.Handler) pi.Handler {
				return func(c *pi.Context) (any, error) {
					order = append(order, "ep-pi")
					return next(c)
				}
			}),
		))
		require.NoError(t, pi.Register(rtGrp, func(ctx *pi.Context, req *slowReq) (*orderRes, error) {
			_, ok := ctx.Deadline()
			hasDeadline.Store(ok)
			<-ctx.Done()
			return nil, ctx.Err()
		}, pi.WithEndpointTimeout(20*time.Millisecond)))

		rec := testkit.Request(t, rtApp, httptest.NewRequest(http.MethodGet, "/v1/order", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, []string{"grp-http", "ep-http", "grp-pi", "ep-pi", "handler"}, order)

		slowRec := testkit.Request(t, rtApp, httptest.NewRequest(http.MethodGet, "/v1/slow", nil))
		require.True(t, hasDeadline.Load())
		require.Equal(t, http.StatusRequestTimeout, slowRec.Code)
	})

	t.Run("statuses 200 201 202 204 empty vs non-empty nullable Res and legacy POST compatibility", func(t *testing.T) {
		app := testkit.New(t) // Note: default LegacyStatus policy for legacy POST

		type res200Req struct {
			pi.Meta `path:"/s200" method:"POST" status:"200"`
		}
		type res201Req struct {
			pi.Meta `path:"/s201" method:"POST" status:"201"`
		}
		type res202Req struct {
			pi.Meta `path:"/s202" method:"POST" status:"202"`
		}
		type res204Req struct {
			pi.Meta `path:"/s204" method:"POST" status:"204"`
		}
		type resEmptyReq struct {
			pi.Meta `path:"/empty" method:"GET"`
		}
		type resNonNullNilReq struct {
			pi.Meta `path:"/nonnull-nil" method:"GET"`
		}
		type resNullableNilReq struct {
			pi.Meta `path:"/nullable-nil" method:"GET" nullable:"true"`
		}
		type dataRes struct {
			Value string `json:"value"`
		}

		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *res200Req) (*dataRes, error) { return &dataRes{Value: "200"}, nil }))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *res201Req) (*dataRes, error) { return &dataRes{Value: "201"}, nil }))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *res202Req) (*dataRes, error) { return &dataRes{Value: "202"}, nil }))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *res204Req) (*emptyRes, error) { return nil, nil }))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *resEmptyReq) (*emptyRes, error) { return nil, nil }))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *resNonNullNilReq) (*dataRes, error) { return nil, nil }))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *resNullableNilReq) (*dataRes, error) { return nil, nil }))

		// Legacy POST without ExplicitHTTPStatus keeps 201 Created.
		app.POST("/legacy-post", func(c *pi.Context) (any, error) { return map[string]string{"legacy": "ok"}, nil })

		require.Equal(t, http.StatusOK, testkit.Request(t, app, httptest.NewRequest(http.MethodPost, "/s200", nil)).Code)
		require.Equal(t, http.StatusCreated, testkit.Request(t, app, httptest.NewRequest(http.MethodPost, "/s201", nil)).Code)
		require.Equal(t, http.StatusAccepted, testkit.Request(t, app, httptest.NewRequest(http.MethodPost, "/s202", nil)).Code)

		r204 := testkit.Request(t, app, httptest.NewRequest(http.MethodPost, "/s204", nil))
		require.Equal(t, http.StatusNoContent, r204.Code)
		require.Empty(t, r204.Body.Bytes())

		rEmpty := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/empty", nil))
		require.Equal(t, http.StatusOK, rEmpty.Code)
		var envEmpty apiEnvelope
		require.NoError(t, json.Unmarshal(rEmpty.Body.Bytes(), &envEmpty))
		require.Equal(t, "null", string(envEmpty.Data))

		rNonNull := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nonnull-nil", nil))
		require.Equal(t, http.StatusInternalServerError, rNonNull.Code)

		rNullable := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nullable-nil", nil))
		require.Equal(t, http.StatusOK, rNullable.Code)
		var envNullable apiEnvelope
		require.NoError(t, json.Unmarshal(rNullable.Body.Bytes(), &envNullable))
		require.Equal(t, "null", string(envNullable.Data))

		rLegacy := testkit.Request(t, app, httptest.NewRequest(http.MethodPost, "/legacy-post", nil))
		require.Equal(t, http.StatusCreated, rLegacy.Code)
	})

	t.Run("strict unknown fields custom validator once 413 415 400 zero secret echo and concurrent race", func(t *testing.T) {
		var customCalls atomic.Int32
		app := testkit.New(t,
			pi.WithConfig(map[string]string{"HTTP_MAX_BODY_BYTES": "64"}),
			pi.WithTypedBindConfig(pi.TypedBindConfig{StrictUnknownFields: true}),
			pi.WithOpenAPI(pi.OpenAPIConfig{Title: "SecAPI", Version: "1.0.0"}),
			pi.WithValidator(func(v any) error {
				customCalls.Add(1)
				return nil
			}),
		)

		type strictReq struct {
			pi.Meta `path:"/strict" method:"POST"`
			Secret  string `json:"secret" validate:"required,min=6"`
		}
		type relaxedReq struct {
			pi.Meta `path:"/relaxed" method:"POST"`
			Name    string `json:"name" validate:"required"`
		}
		type okRes struct {
			Len int `json:"len"`
		}

		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *strictReq) (*okRes, error) {
			return &okRes{Len: len(req.Secret)}, nil
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *relaxedReq) (*okRes, error) {
			return &okRes{Len: len(req.Name)}, nil
		}, pi.WithStrictUnknownFields(false)))

		// Valid request invokes custom validator exactly once.
		okReq := httptest.NewRequest(http.MethodPost, "/strict", strings.NewReader(`{"secret":"topsecret"}`))
		okReq.Header.Set("Content-Type", "application/json")
		okResp := testkit.Request(t, app, okReq)
		require.Equal(t, http.StatusOK, okResp.Code)
		require.EqualValues(t, 1, customCalls.Load())

		// Unknown key rejected in strict mode.
		unkReq := httptest.NewRequest(http.MethodPost, "/strict", strings.NewReader(`{"secret":"topsecret","extra":1}`))
		unkReq.Header.Set("Content-Type", "application/json")
		require.Equal(t, http.StatusBadRequest, testkit.Request(t, app, unkReq).Code)

		// Unknown key accepted when overridden with WithStrictUnknownFields(false).
		relReq := httptest.NewRequest(http.MethodPost, "/relaxed", strings.NewReader(`{"name":"ok","extra":1}`))
		relReq.Header.Set("Content-Type", "application/json")
		require.Equal(t, http.StatusOK, testkit.Request(t, app, relReq).Code)

		// 415 Unsupported Media Type.
		xmlReq := httptest.NewRequest(http.MethodPost, "/strict", strings.NewReader(`{"secret":"topsecret"}`))
		xmlReq.Header.Set("Content-Type", "text/plain")
		require.Equal(t, http.StatusUnsupportedMediaType, testkit.Request(t, app, xmlReq).Code)

		// 413 Payload Too Large (> 64 bytes).
		bigBody := `{"secret":"` + strings.Repeat("a", 80) + `"}`
		bigReq := httptest.NewRequest(http.MethodPost, "/strict", strings.NewReader(bigBody))
		bigReq.Header.Set("Content-Type", "application/json")
		require.Equal(t, http.StatusRequestEntityTooLarge, testkit.Request(t, app, bigReq).Code)

		// 400 validation error never echoes raw submitted secret value.
		shortSecret := "xy!"
		badSecretReq := httptest.NewRequest(http.MethodPost, "/strict", strings.NewReader(`{"secret":"`+shortSecret+`"}`))
		badSecretReq.Header.Set("Content-Type", "application/json")
		badResp := testkit.Request(t, app, badSecretReq)
		require.Equal(t, http.StatusBadRequest, badResp.Code)
		require.NotContains(t, badResp.Body.String(), shortSecret)
		require.Contains(t, badResp.Body.String(), `"field":"secret"`)

		// Concurrent HTTP requests + concurrent OpenAPIJSON calls under -race.
		h, err := app.HTTPHandler()
		require.NoError(t, err)
		var wg sync.WaitGroup
		for range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest(http.MethodPost, "/relaxed", strings.NewReader(`{"name":"concurrent"}`))
				req.Header.Set("Content-Type", "application/json")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Errorf("unexpected status %d", rec.Code)
				}
				if _, err := app.OpenAPIJSON(); err != nil {
					t.Errorf("OpenAPIJSON error: %v", err)
				}
			}()
		}
		wg.Wait()
	})

	t.Run("custom validator error returned cleanly", func(t *testing.T) {
		app := testkit.New(t, pi.WithValidator(func(v any) error {
			return apperror.New(apperror.InvalidArgument, http.StatusBadRequest, "custom validation failed").
				WithDetails(apperror.Detail{Field: "secret", Rule: "custom"})
		}))
		type reqDTO struct {
			pi.Meta `path:"/custom" method:"POST"`
			Secret  string `json:"secret"`
		}
		type resDTO struct {
			OK bool `json:"ok"`
		}
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *reqDTO) (*resDTO, error) {
			return &resDTO{OK: true}, nil
		}))
		req := httptest.NewRequest(http.MethodPost, "/custom", strings.NewReader(`{"secret":"hunter2"}`))
		req.Header.Set("Content-Type", "application/json")
		rec := testkit.Request(t, app, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.NotContains(t, rec.Body.String(), "hunter2")
		require.Contains(t, rec.Body.String(), `"field":"secret"`)
	})
}

type benchReq struct {
	pi.Meta `path:"/bench/{id}" method:"POST"`
	ID      string `path:"id" validate:"required"`
	Filter  string `query:"filter"`
	Name    string `json:"name" validate:"required"`
	Count   int    `json:"count" validate:"gte=0"`
}

type benchRes struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type benchController struct{}

func (b *benchController) Handle(ctx *pi.Context, req *benchReq) (*benchRes, error) {
	return &benchRes{ID: req.ID, Name: req.Name, Count: req.Count}, nil
}

func BenchmarkTypedRegister(b *testing.B) {
	app, err := pi.Build(
		pi.WithConfig(map[string]string{"LOG_LEVEL": "ERROR", "PI_TELEMETRY": "false"}),
		pi.WithExplicitHTTPStatus(),
	)
	if err != nil {
		b.Fatal(err)
	}
	ctrl := &benchController{}
	if err := pi.Register(app, ctrl.Handle); err != nil {
		b.Fatal(err)
	}
	h, err := app.HTTPHandler()
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(`{"name":"widget","count":3}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/bench/123?filter=active", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkTypedGroupBind(b *testing.B) {
	app, err := pi.Build(
		pi.WithConfig(map[string]string{"LOG_LEVEL": "ERROR", "PI_TELEMETRY": "false"}),
		pi.WithExplicitHTTPStatus(),
	)
	if err != nil {
		b.Fatal(err)
	}
	ctrl := &benchController{}
	if err := app.Group("/").Bind(ctrl); err != nil {
		b.Fatal(err)
	}
	h, err := app.HTTPHandler()
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(`{"name":"widget","count":3}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/bench/123?filter=active", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkLegacyManualBind(b *testing.B) {
	app, err := pi.Build(
		pi.WithConfig(map[string]string{"LOG_LEVEL": "ERROR", "PI_TELEMETRY": "false"}),
		pi.WithExplicitHTTPStatus(),
	)
	if err != nil {
		b.Fatal(err)
	}
	type legacyBody struct {
		Name  string `json:"name" binding:"required"`
		Count int    `json:"count" binding:"gte=0"`
	}
	app.POST("/bench/{id}", func(ctx *pi.Context) (any, error) {
		id := ctx.PathParam("id")
		_ = ctx.Param("filter")
		var body legacyBody
		if err := ctx.Bind(&body); err != nil {
			return nil, err
		}
		return &benchRes{ID: id, Name: body.Name, Count: body.Count}, nil
	})
	h, err := app.HTTPHandler()
	if err != nil {
		b.Fatal(err)
	}
	payload := []byte(`{"name":"widget","count":3}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, "/bench/123?filter=active", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
	}
}

func TestIssue1LegacyRegexpConflictNormalization(t *testing.T) {
	t.Run("distinct regex path params coexist in legacy routes", func(t *testing.T) {
		app := testkit.New(t)
		app.GET("/users/{id:[0-9]+}", func(ctx *pi.Context) (any, error) {
			return map[string]string{"id": ctx.PathParam("id")}, nil
		})
		app.GET("/users/{slug:[a-z]+}", func(ctx *pi.Context) (any, error) {
			return map[string]string{"slug": ctx.PathParam("slug")}, nil
		})

		require.NoError(t, app.RouteError())
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		recID := httptest.NewRecorder()
		h.ServeHTTP(recID, httptest.NewRequest(http.MethodGet, "/users/42", nil))
		require.Equal(t, http.StatusOK, recID.Code)
		require.Contains(t, recID.Body.String(), `"42"`)

		recSlug := httptest.NewRecorder()
		h.ServeHTTP(recSlug, httptest.NewRequest(http.MethodGet, "/users/alice", nil))
		require.Equal(t, http.StatusOK, recSlug.Code)
		require.Contains(t, recSlug.Body.String(), `"alice"`)
	})

	t.Run("duplicate regex constraints fail RouteError and HTTPHandler", func(t *testing.T) {
		app := testkit.New(t)
		app.GET("/users/{id:[0-9]+}", func(ctx *pi.Context) (any, error) {
			return "id", nil
		})
		app.GET("/users/{uid:[0-9]+}", func(ctx *pi.Context) (any, error) {
			return "uid", nil
		})

		err := app.RouteError()
		require.Error(t, err)
		h, err := app.HTTPHandler()
		require.Error(t, err)
		require.Nil(t, h)
	})
}

func TestIssue2ParameterGroupMountCollisions(t *testing.T) {
	type profileReq struct {
		pi.Meta `path:"/profile" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type settingsReq struct {
		pi.Meta `path:"/settings" method:"GET" tags:"users"`
		Name    string `path:"name" validate:"required"`
	}
	type sameParamSettingsReq struct {
		pi.Meta `path:"/settings" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type okRes struct {
		Value string `json:"value"`
	}

	t.Run("upfront empty conflicting parameter groups allow first Register and reject second cleanly", func(t *testing.T) {
		app := testkit.New(t)
		idGroup := app.Group("/users/{id}")
		nameGroup := app.Group("/users/{name}")

		require.NoError(t, pi.Register(idGroup, func(ctx *pi.Context, req *profileReq) (*okRes, error) {
			return &okRes{Value: req.ID}, nil
		}))

		err := pi.Register(nameGroup, func(ctx *pi.Context, req *settingsReq) (*okRes, error) {
			return &okRes{Value: req.Name}, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "group mount conflict")
		require.Error(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.Error(t, err)
		require.Nil(t, h)
	})

	t.Run("nested and flat conflicting parameter groups are rejected without chi panic", func(t *testing.T) {
		app := testkit.New(t)
		users := app.Group("/users")
		idGroup := users.Group("/{id}")
		nameGroup := app.Group("/users/{name}")

		require.NoError(t, pi.Register(idGroup, func(ctx *pi.Context, req *profileReq) (*okRes, error) {
			return &okRes{Value: req.ID}, nil
		}))

		err := pi.Register(nameGroup, func(ctx *pi.Context, req *settingsReq) (*okRes, error) {
			return &okRes{Value: req.Name}, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "/users/{}")
		require.Error(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.Error(t, err)
		require.Nil(t, h)
	})

	t.Run("pure legacy conflicting parameter groups fail RouteError and HTTPHandler without panic", func(t *testing.T) {
		app := testkit.New(t)
		idGroup := app.Group("/users/{id}")
		nameGroup := app.Group("/users/{name}")

		idGroup.GET("/profile", func(ctx *pi.Context) (any, error) {
			return "profile", nil
		})
		nameGroup.GET("/settings", func(ctx *pi.Context) (any, error) {
			return "settings", nil
		})

		err := app.RouteError()
		require.Error(t, err)
		require.Contains(t, err.Error(), "group mount conflict")

		h, err := app.HTTPHandler()
		require.Error(t, err)
		require.Nil(t, h)
	})

	t.Run("unused empty parameter group does not block active sibling group", func(t *testing.T) {
		app := testkit.New(t)
		_ = app.Group("/users/{unused}")
		idGroup := app.Group("/users/{id}")

		require.NoError(t, pi.Register(idGroup, func(ctx *pi.Context, req *profileReq) (*okRes, error) {
			return &okRes{Value: req.ID}, nil
		}))
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/users/99/profile", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), `"99"`)
	})

	t.Run("two group handles with identical param prefix coexist cleanly", func(t *testing.T) {
		app := testkit.New(t)
		g1 := app.Group("/users/{id}")
		g2 := app.Group("/users/{id}")

		require.NoError(t, pi.Register(g1, func(ctx *pi.Context, req *profileReq) (*okRes, error) {
			return &okRes{Value: "profile:" + req.ID}, nil
		}))
		require.NoError(t, pi.Register(g2, func(ctx *pi.Context, req *sameParamSettingsReq) (*okRes, error) {
			return &okRes{Value: "settings:" + req.ID}, nil
		}))
		require.NoError(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		rec1 := httptest.NewRecorder()
		h.ServeHTTP(rec1, httptest.NewRequest(http.MethodGet, "/users/7/profile", nil))
		require.Equal(t, http.StatusOK, rec1.Code)
		require.Contains(t, rec1.Body.String(), `"profile:7"`)

		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, httptest.NewRequest(http.MethodGet, "/users/7/settings", nil))
		require.Equal(t, http.StatusOK, rec2.Code)
		require.Contains(t, rec2.Body.String(), `"settings:7"`)
	})
}

func TestIssue5CrossMethodParameterNameConsistency(t *testing.T) {
	type getUserByIDReq struct {
		pi.Meta `path:"/users/{id}" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type postUserByNameReq struct {
		pi.Meta `path:"/users/{name}" method:"POST" tags:"users"`
		Name    string `path:"name" validate:"required"`
	}
	type postUserByIDReq struct {
		pi.Meta `path:"/users/{id}" method:"POST" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type okRes struct {
		ID string `json:"id"`
	}

	t.Run("mismatched path parameter names across HTTP methods are rejected", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "ParamMismatch", Version: "1.0.0"}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *getUserByIDReq) (*okRes, error) {
			return &okRes{ID: req.ID}, nil
		}))

		err := pi.Register(app, func(ctx *pi.Context, req *postUserByNameReq) (*okRes, error) {
			return &okRes{ID: req.Name}, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "/users/{}")
		require.Error(t, app.RouteError())

		spec, err := app.OpenAPISpec()
		require.Error(t, err)
		require.Nil(t, spec)
	})

	t.Run("consistent path parameter names across HTTP methods merge on same PathItem", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "ParamMatch", Version: "1.0.0"}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *getUserByIDReq) (*okRes, error) {
			return &okRes{ID: req.ID}, nil
		}))
		require.NoError(t, pi.Register(app, func(ctx *pi.Context, req *postUserByIDReq) (*okRes, error) {
			return &okRes{ID: req.ID}, nil
		}))
		require.NoError(t, app.RouteError())

		spec, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Contains(t, spec.Paths, "/users/{id}")
		require.NotNil(t, spec.Paths["/users/{id}"].Get)
		require.NotNil(t, spec.Paths["/users/{id}"].Post)
	})
}

func TestNestedAndFlatParameterGroupsCoexist(t *testing.T) {
	type nestedProfileReq struct {
		pi.Meta `path:"/profile" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type flatSettingsReq struct {
		pi.Meta `path:"/settings" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type ordersListReq struct {
		pi.Meta `path:"/v1/orders" method:"GET" tags:"orders"`
	}
	type v1UsersListReq struct {
		pi.Meta `path:"/users" method:"GET" tags:"users"`
	}
	type postByNameReq struct {
		pi.Meta `path:"/items" method:"GET" tags:"users"`
		Name    string `path:"name" validate:"required"`
	}
	type strRes struct {
		Value string `json:"value"`
	}

	runCoexist := func(t *testing.T, nestedFirst bool) {
		t.Helper()
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "NestedFlatCoexist", Version: "1.0.0"}))

		registerNested := func() {
			idGroup := app.Group("/users").Group("/{id}")
			require.NoError(t, pi.Register(idGroup, func(ctx *pi.Context, req *nestedProfileReq) (*strRes, error) {
				return &strRes{Value: "profile:" + req.ID}, nil
			}))
		}
		registerFlat := func() {
			flatGroup := app.Group("/users/{id}")
			require.NoError(t, pi.Register(flatGroup, func(ctx *pi.Context, req *flatSettingsReq) (*strRes, error) {
				return &strRes{Value: "settings:" + req.ID}, nil
			}))
		}

		if nestedFirst {
			registerNested()
			registerFlat()
		} else {
			registerFlat()
			registerNested()
		}

		require.NoError(t, app.RouteError())

		rawSpec, err := app.OpenAPIJSON()
		require.NoError(t, err)
		require.Contains(t, string(rawSpec), "/users/{id}/profile")
		require.Contains(t, string(rawSpec), "/users/{id}/settings")

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		recProfile := httptest.NewRecorder()
		h.ServeHTTP(recProfile, httptest.NewRequest(http.MethodGet, "/users/42/profile", nil))
		require.Equal(t, http.StatusOK, recProfile.Code)
		require.Contains(t, recProfile.Body.String(), `"profile:42"`)

		recSettings := httptest.NewRecorder()
		h.ServeHTTP(recSettings, httptest.NewRequest(http.MethodGet, "/users/99/settings", nil))
		require.Equal(t, http.StatusOK, recSettings.Code)
		require.Contains(t, recSettings.Body.String(), `"settings:99"`)
	}

	t.Run("nested group registered before flat group", func(t *testing.T) {
		runCoexist(t, true)
	})
	t.Run("flat group registered before nested group", func(t *testing.T) {
		runCoexist(t, false)
	})

	t.Run("overlapping static prefix typed groups serve all routes", func(t *testing.T) {
		app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "StaticOverlap", Version: "1.0.0"}))
		apiGroup := app.Group("/api")
		v1Group := app.Group("/api/v1")

		require.NoError(t, pi.Register(apiGroup, func(ctx *pi.Context, _ *ordersListReq) (*strRes, error) {
			return &strRes{Value: "orders"}, nil
		}))
		require.NoError(t, pi.Register(v1Group, func(ctx *pi.Context, _ *v1UsersListReq) (*strRes, error) {
			return &strRes{Value: "users"}, nil
		}))
		require.NoError(t, app.RouteError())

		spec, err := app.OpenAPISpec()
		require.NoError(t, err)
		require.Contains(t, spec.Paths, "/api/v1/orders")
		require.Contains(t, spec.Paths, "/api/v1/users")

		h, err := app.HTTPHandler()
		require.NoError(t, err)

		recOrders := httptest.NewRecorder()
		h.ServeHTTP(recOrders, httptest.NewRequest(http.MethodGet, "/api/v1/orders", nil))
		require.Equal(t, http.StatusOK, recOrders.Code)
		require.Contains(t, recOrders.Body.String(), `"orders"`)

		recUsers := httptest.NewRecorder()
		h.ServeHTTP(recUsers, httptest.NewRequest(http.MethodGet, "/api/v1/users", nil))
		require.Equal(t, http.StatusOK, recUsers.Code)
		require.Contains(t, recUsers.Body.String(), `"users"`)
	})

	t.Run("multi-segment intermediate parameter conflict is rejected", func(t *testing.T) {
		app := testkit.New(t)
		require.NoError(t, pi.Register(app.Group("/users/{id}/posts"), func(ctx *pi.Context, req *nestedProfileReq) (*strRes, error) {
			return &strRes{Value: req.ID}, nil
		}))

		err := pi.Register(app.Group("/users/{name}/comments"), func(ctx *pi.Context, req *postByNameReq) (*strRes, error) {
			return &strRes{Value: req.Name}, nil
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "group mount conflict")
		require.Contains(t, err.Error(), "/users/{}")
		require.Error(t, app.RouteError())

		h, err := app.HTTPHandler()
		require.Error(t, err)
		require.Nil(t, h)
	})
}

func TestLegacyRegexpGroupsAndPrecheck(t *testing.T) {
	t.Run("distinct legacy regex groups coexist and serve 200 OK", func(t *testing.T) {
		app := testkit.New(t)
		app.Group("/users/{id:[0-9]+}").GET("/profile", func(ctx *pi.Context) (any, error) {
			return map[string]string{"id": ctx.PathParam("id")}, nil
		})
		app.Group("/users/{slug:[a-z]+}").GET("/settings", func(ctx *pi.Context) (any, error) {
			return map[string]string{"slug": ctx.PathParam("slug")}, nil
		})

		require.NoError(t, app.RouteError())
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		recID := httptest.NewRecorder()
		h.ServeHTTP(recID, httptest.NewRequest(http.MethodGet, "/users/123/profile", nil))
		require.Equal(t, http.StatusOK, recID.Code)
		require.Contains(t, recID.Body.String(), `"123"`)

		recSlug := httptest.NewRecorder()
		h.ServeHTTP(recSlug, httptest.NewRequest(http.MethodGet, "/users/alice/settings", nil))
		require.Equal(t, http.StatusOK, recSlug.Code)
		require.Contains(t, recSlug.Body.String(), `"alice"`)

		recMiss := httptest.NewRecorder()
		h.ServeHTTP(recMiss, httptest.NewRequest(http.MethodGet, "/users/abc123/profile", nil))
		require.Equal(t, http.StatusNotFound, recMiss.Code)
	})

	t.Run("identical legacy regex groups with different param names fail RouteError and HTTPHandler", func(t *testing.T) {
		app := testkit.New(t)
		app.Group("/users/{id:[0-9]+}").GET("/profile", func(ctx *pi.Context) (any, error) {
			return "id", nil
		})
		app.Group("/users/{uid:[0-9]+}").GET("/settings", func(ctx *pi.Context) (any, error) {
			return "uid", nil
		})

		err := app.RouteError()
		require.Error(t, err)
		require.Contains(t, err.Error(), "group mount conflict")

		h, err := app.HTTPHandler()
		require.Error(t, err)
		require.Nil(t, h)
	})

	t.Run("malformed legacy chi routes fail RouteError and HTTPHandler without panicking", func(t *testing.T) {
		cases := []struct {
			name    string
			setup   func(app *pi.App)
			wantErr string
		}{
			{
				name: "unclosed brace",
				setup: func(app *pi.App) {
					app.GET("/users/{id", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "unclosed '{'",
			},
			{
				name: "unmatched closing brace",
				setup: func(app *pi.App) {
					app.GET("/users/id}", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "unmatched '}'",
			},
			{
				name: "invalid regexp",
				setup: func(app *pi.App) {
					app.GET("/users/{id:[}", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "invalid regexp",
			},
			{
				name: "duplicate param in single route",
				setup: func(app *pi.App) {
					app.GET("/users/{id}/posts/{id}", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "duplicate path parameter",
			},
			{
				name: "duplicate param across group and route",
				setup: func(app *pi.App) {
					app.Group("/users/{id}").GET("/posts/{id}", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "duplicate path parameter",
			},
			{
				name: "non-terminal wildcard outside braces",
				setup: func(app *pi.App) {
					app.GET("/users/*/posts", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "wildcard '*'",
			},
			{
				name: "empty param name",
				setup: func(app *pi.App) {
					app.GET("/users/{}", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "invalid parameter name",
			},
			{
				name: "adjacent params without delimiter",
				setup: func(app *pi.App) {
					app.GET("/users/{a}{b}", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "adjacent parameters",
			},
			{
				name: "adjacent param and wildcard without delimiter",
				setup: func(app *pi.App) {
					app.GET("/users/{a}*", func(ctx *pi.Context) (any, error) { return "ok", nil })
				},
				wantErr: "adjacent parameters",
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				app := testkit.New(t, pi.WithOpenAPI(pi.OpenAPIConfig{Title: "LegacyPrecheck", Version: "1.0.0"}))
				tc.setup(app)

				require.NotPanics(t, func() {
					err := app.RouteError()
					require.Error(t, err)
					require.Contains(t, err.Error(), tc.wantErr)

					h, err := app.HTTPHandler()
					require.Error(t, err)
					require.Nil(t, h)

					spec, err := app.OpenAPISpec()
					require.Error(t, err)
					require.Nil(t, spec)
				})
			})
		}
	})

	t.Run("valid legacy regex quantifiers, regex asterisk, and trailing wildcard work", func(t *testing.T) {
		app := testkit.New(t)
		app.GET("/items/{code:[0-9]{2,4}}/detail", func(ctx *pi.Context) (any, error) {
			return ctx.PathParam("code"), nil
		})
		app.GET("/digits/{num:[0-9]*}/end", func(ctx *pi.Context) (any, error) {
			return ctx.PathParam("num"), nil
		})
		app.GET("/files/*", func(ctx *pi.Context) (any, error) {
			return ctx.PathParam("*"), nil
		})

		require.NoError(t, app.RouteError())
		h, err := app.HTTPHandler()
		require.NoError(t, err)

		recItem := httptest.NewRecorder()
		h.ServeHTTP(recItem, httptest.NewRequest(http.MethodGet, "/items/123/detail", nil))
		require.Equal(t, http.StatusOK, recItem.Code)
		require.Contains(t, recItem.Body.String(), "123")

		recDigits := httptest.NewRecorder()
		h.ServeHTTP(recDigits, httptest.NewRequest(http.MethodGet, "/digits/45/end", nil))
		require.Equal(t, http.StatusOK, recDigits.Code)
		require.Contains(t, recDigits.Body.String(), "45")

		recFiles := httptest.NewRecorder()
		h.ServeHTTP(recFiles, httptest.NewRequest(http.MethodGet, "/files/a/b.txt", nil))
		require.Equal(t, http.StatusOK, recFiles.Code)
		require.Contains(t, recFiles.Body.String(), "a/b.txt")
	})
}
