package contract_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/apperror"
	piHTTP "github.com/sllt/pi/pkg/pi/http"
	"github.com/sllt/pi/pkg/pi/http/contract"
	resTypes "github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/openapi"
)

type emptyRes struct{}

type simpleRes struct {
	OK bool `json:"ok"`
}

type Tag struct {
	Label string `json:"label"`
}

type UserItem struct {
	ID    string `json:"id"`
	Email string `json:"email"`
}

type PageRes[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

type treeNodeRes struct {
	Label string       `json:"label"`
	Next  *treeNodeRes `json:"next,omitempty"`
}

type nullableChild struct {
	Code string `json:"code"`
}

func makePIRequest(method, target string, body []byte, contentType string, pathParams map[string]string) *piHTTP.Request {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, bodyReader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if len(pathParams) > 0 {
		rctx := chi.NewRouteContext()
		for k, v := range pathParams {
			rctx.URLParams.Add(k, v)
		}
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	}
	return piHTTP.NewRequestWithSnapshot(req, nil, body, body != nil)
}

func makePIRequestWithHeaders(method, target string, body []byte, contentType string, pathParams map[string]string, headers http.Header) *piHTTP.Request {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req := httptest.NewRequest(method, target, bodyReader)
	for k, vals := range headers {
		for _, v := range vals {
			req.Header.Add(k, v)
		}
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if len(pathParams) > 0 {
		rctx := chi.NewRouteContext()
		for k, v := range pathParams {
			rctx.URLParams.Add(k, v)
		}
		req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	}
	return piHTTP.NewRequestWithSnapshot(req, nil, body, body != nil)
}

func compileOp[Req any, Res any](t *testing.T, opts contract.CompileOptions) (*contract.CompiledOperation, *contract.SchemaRegistry) {
	t.Helper()
	sr := contract.NewSchemaRegistry()
	op, err := contract.CompileOperation(
		t.Name(),
		reflect.TypeOf((*Req)(nil)),
		reflect.TypeOf((*Res)(nil)),
		opts,
		sr,
	)
	require.NoError(t, err)
	return op, sr
}

func TestCompileOperation_MetaPathAndConflictRules(t *testing.T) {
	assert.True(t, contract.IsPiMetaType(reflect.TypeOf(pi.Meta{})))
	assert.False(t, contract.IsPiMetaType(reflect.TypeOf(&pi.Meta{})))
	assert.False(t, contract.IsPiMetaType(reflect.TypeOf(struct{}{})))

	assert.Equal(t, "", contract.NormalizeGroupPrefix(""))
	assert.Equal(t, "", contract.NormalizeGroupPrefix("/"))
	assert.Equal(t, "/api/v1", contract.NormalizeGroupPrefix("api/v1/"))
	assert.Equal(t, "/api/v1/user", contract.NormalizeGroupPrefix("/api//v1/user/"))

	canon, shape := contract.NormalizeRouteConflict("/api/v1/user", "get", "/")
	assert.Equal(t, "/api/v1/user", canon)
	assert.Equal(t, "GET /api/v1/user", shape)

	_, shapeA := contract.NormalizeRouteConflict("/api/v1", "GET", "/users/{id}")
	_, shapeB := contract.NormalizeRouteConflict("/api/v1", "get", "/users/{userId}")
	assert.Equal(t, "GET /api/v1/users/{}", shapeA)
	assert.Equal(t, shapeA, shapeB)

	canonSub, shapeSub := contract.NormalizeRouteConflict("/api/v1/user", "POST", "/profile/")
	assert.Equal(t, "/api/v1/user/profile/", canonSub)
	assert.Equal(t, "POST /api/v1/user/profile/", shapeSub)

	t.Run("meta_structure_validation", func(t *testing.T) {
		type missingMetaReq struct {
			Name string `json:"name"`
		}
		type pointerMetaReq struct {
			*pi.Meta `path:"/x" method:"GET"`
		}
		type namedMetaReq struct {
			Meta pi.Meta `path:"/x" method:"GET"`
		}
		type dupMetaReq struct {
			pi.Meta `path:"/x" method:"GET"`
			Second  pi.Meta `path:"/y" method:"GET"`
		}
		type embeddedMeta struct {
			pi.Meta `path:"/x" method:"GET"`
		}
		type nestedMetaReq struct {
			embeddedMeta
		}
		type okReq struct {
			pi.Meta `path:"/x" method:"GET"`
		}
		type resWithMeta struct {
			pi.Meta `path:"/x" method:"GET"`
			ID      string `json:"id"`
		}

		type nestedFileInReq struct {
			pi.Meta `path:"/x" method:"POST"`
			F       *resTypes.File `json:"f"`
		}
		type nestedXMLInRes struct {
			X *resTypes.XML `json:"x"`
		}

		cases := []struct {
			name string
			req  reflect.Type
			res  reflect.Type
		}{
			{"missing_meta", reflect.TypeOf((*missingMetaReq)(nil)), reflect.TypeOf((*emptyRes)(nil))},
			{"pointer_meta", reflect.TypeOf((*pointerMetaReq)(nil)), reflect.TypeOf((*emptyRes)(nil))},
			{"named_meta", reflect.TypeOf((*namedMetaReq)(nil)), reflect.TypeOf((*emptyRes)(nil))},
			{"duplicate_meta", reflect.TypeOf((*dupMetaReq)(nil)), reflect.TypeOf((*emptyRes)(nil))},
			{"nested_embedded_meta", reflect.TypeOf((*nestedMetaReq)(nil)), reflect.TypeOf((*emptyRes)(nil))},
			{"res_with_meta", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resWithMeta)(nil))},
			{"nil_req_type", nil, reflect.TypeOf((*emptyRes)(nil))},
			{"non_ptr_req_type", reflect.TypeOf(okReq{}), reflect.TypeOf((*emptyRes)(nil))},
			{"forbidden_res_result", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Result)(nil))},
			{"forbidden_res_stream", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Stream)(nil))},
			{"nested_file_in_req", reflect.TypeOf((*nestedFileInReq)(nil)), reflect.TypeOf((*emptyRes)(nil))},
			{"forbidden_res_raw", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Raw)(nil))},
			{"forbidden_res_template", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Template)(nil))},
			{"nested_xml_in_res", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*nestedXMLInRes)(nil))},
			{"forbidden_res_redirect", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Redirect)(nil))},
			{"forbidden_res_response", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Response)(nil))},
			{"forbidden_res_handled", reflect.TypeOf((*okReq)(nil)), reflect.TypeOf((*resTypes.Handled)(nil))},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := contract.CompileOperation("h", tc.req, tc.res, contract.CompileOptions{}, contract.NewSchemaRegistry())
				assert.Error(t, err)
			})
		}
	})

	t.Run("method_status_path_and_pairing_rules", func(t *testing.T) {
		type noMethodReq struct {
			pi.Meta `path:"/x"`
		}
		type badMethodReq struct {
			pi.Meta `path:"/x" method:"TRACE"`
		}
		type badPathQueryReq struct {
			pi.Meta `path:"/x?a=1" method:"GET"`
		}
		type badPathWildcardReq struct {
			pi.Meta `path:"/x/*" method:"GET"`
		}
		type badPathRegexReq struct {
			pi.Meta `path:"/users/{id:[0-9]+}" method:"GET"`
			ID      string `path:"id"`
		}
		type badPathDotReq struct {
			pi.Meta `path:"/users/../secret" method:"GET"`
		}
		type rootEmptyPathReq struct {
			pi.Meta `path:"" method:"GET"`
		}
		type getWithBodyReq struct {
			pi.Meta `path:"/users" method:"GET"`
			Name    string `json:"name"`
		}
		type deleteWithBodyReq struct {
			pi.Meta `path:"/users" method:"DELETE"`
			Reason  string `json:"reason"`
		}
		type badStatusReq struct {
			pi.Meta `path:"/users" method:"POST" status:"400"`
		}
		type status204NonEmptyResReq struct {
			pi.Meta `path:"/users" method:"PUT" status:"204"`
		}
		type missingPathFieldReq struct {
			pi.Meta `path:"/users/{id}" method:"GET"`
		}
		type extraPathFieldReq struct {
			pi.Meta `path:"/users" method:"GET"`
			ID      string `path:"id"`
		}

		invalid := []struct {
			name string
			req  reflect.Type
			res  reflect.Type
			opts contract.CompileOptions
		}{
			{"missing_method", reflect.TypeOf((*noMethodReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"unsupported_method", reflect.TypeOf((*badMethodReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"path_with_query", reflect.TypeOf((*badPathQueryReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"path_with_wildcard", reflect.TypeOf((*badPathWildcardReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"path_with_regex", reflect.TypeOf((*badPathRegexReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"path_with_dotdot", reflect.TypeOf((*badPathDotReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"root_empty_path", reflect.TypeOf((*rootEmptyPathReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{GroupPrefix: ""}},
			{"get_with_body", reflect.TypeOf((*getWithBodyReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"delete_with_body", reflect.TypeOf((*deleteWithBodyReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"unsupported_status", reflect.TypeOf((*badStatusReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"status_204_nonempty_res", reflect.TypeOf((*status204NonEmptyResReq)(nil)), reflect.TypeOf((*simpleRes)(nil)), contract.CompileOptions{}},
			{"missing_path_field", reflect.TypeOf((*missingPathFieldReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
			{"extra_path_field", reflect.TypeOf((*extraPathFieldReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}},
		}
		for _, tc := range invalid {
			t.Run(tc.name, func(t *testing.T) {
				_, err := contract.CompileOperation("h", tc.req, tc.res, tc.opts, contract.NewSchemaRegistry())
				assert.Error(t, err)
			})
		}

		// Group empty/slash canonicalization vs subpath trailing slash preservation
		type groupRootEmptyReq struct {
			pi.Meta `path:"" method:"GET"`
		}
		type groupRootSlashReq struct {
			pi.Meta `path:"/" method:"GET"`
		}
		type groupSubSlashReq struct {
			pi.Meta `path:"/profile/" method:"GET"`
		}
		opEmpty, _ := compileOp[groupRootEmptyReq, emptyRes](t, contract.CompileOptions{GroupPrefix: "/api/v1/user"})
		assert.Equal(t, "/", opEmpty.ChiPattern)
		assert.Equal(t, "/api/v1/user", opEmpty.CanonicalPath)
		assert.Equal(t, "GET /api/v1/user", opEmpty.ConflictShape)

		opSlash, _ := compileOp[groupRootSlashReq, emptyRes](t, contract.CompileOptions{GroupPrefix: "/api/v1/user"})
		assert.Equal(t, "/", opSlash.ChiPattern)
		assert.Equal(t, "/api/v1/user", opSlash.CanonicalPath)

		opSub, _ := compileOp[groupSubSlashReq, emptyRes](t, contract.CompileOptions{GroupPrefix: "/api/v1/user"})
		assert.Equal(t, "/profile/", opSub.ChiPattern)
		assert.Equal(t, "/api/v1/user/profile/", opSub.CanonicalPath)

		// 204 allowed when Res only has json:"-" and unexported fields
		type hiddenOnlyRes struct {
			internal int
			Secret   string `json:"-"`
		}
		type ok204Req struct {
			pi.Meta `path:"/user" method:"PUT" status:"204"`
		}
		op204, _ := compileOp[ok204Req, hiddenOnlyRes](t, contract.CompileOptions{GroupPrefix: "/api/v1"})
		assert.Equal(t, 204, op204.Status)
		assert.True(t, op204.EmptyResponse)

		// GroupPrefix placeholder paired with req field
		type tenantReq struct {
			pi.Meta  `path:"/users/{id}" method:"get" operationId:""`
			TenantID string `path:"tenantId"`
			ID       string `path:"id"`
		}
		// operationId:"" explicitly empty should fail; test normal generated ID separately
		_, err := contract.CompileOperation("h", reflect.TypeOf((*tenantReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{GroupPrefix: "/tenants/{tenantId}"}, contract.NewSchemaRegistry())
		assert.Error(t, err)

		type tenantAutoOpIDReq struct {
			pi.Meta  `path:"/users/{id}" method:"get"`
			TenantID string `path:"tenantId"`
			ID       string `path:"id"`
		}
		opAuto, _ := compileOp[tenantAutoOpIDReq, emptyRes](t, contract.CompileOptions{GroupPrefix: "/tenants/{tenantId}"})
		assert.Equal(t, "GET", opAuto.Method)
		assert.Equal(t, "getTenantsByTenantIdUsersById", opAuto.OperationID)

		type userByIDReq struct {
			pi.Meta `path:"/users/{id}" method:"GET"`
			ID      string `path:"id"`
		}
		opUserID, _ := compileOp[userByIDReq, emptyRes](t, contract.CompileOptions{GroupPrefix: "/api/v1"})
		assert.Equal(t, "getApiV1UsersById", opUserID.OperationID)

		type customOpIDReq struct {
			pi.Meta `path:"/users/{id}" method:"GET" operationId:"fetchUser"`
			ID      string `path:"id"`
		}
		opCustomID, _ := compileOp[customOpIDReq, emptyRes](t, contract.CompileOptions{GroupPrefix: "/api/v1"})
		assert.Equal(t, "fetchUser", opCustomID.OperationID)
	})

	t.Run("tags_security_and_strict_inheritance", func(t *testing.T) {
		type baseReq struct {
			pi.Meta `path:"/items" method:"POST"`
			Name    string `json:"name"`
		}
		type overrideReq struct {
			pi.Meta `path:"/items" method:"POST" tags:"MetaTag" security:"Bearer,ApiKey" strict:"false" nullable:"true" errors:"409,422"`
			Name    string `json:"name"`
		}
		type noSecReq struct {
			pi.Meta `path:"/public" method:"GET" security:"-"`
		}

		// Group inherited -> Endpoint overrides -> Meta overrides
		opInherited, _ := compileOp[baseReq, simpleRes](t, contract.CompileOptions{
			DefaultStrictJSON: true,
			InheritedTags:     []string{"GroupTag"},
			HasInheritedSec:   true,
			InheritedSecurity: []string{"Bearer"},
			HasEndpointTags:   true,
			EndpointTags:      []string{"EndpointTag"},
			HasEndpointSec:    true,
			EndpointSecurity:  []string{"OAuth2"},
		})
		assert.Equal(t, []string{"EndpointTag"}, opInherited.Tags)
		assert.True(t, opInherited.StrictJSON)
		require.NotNil(t, opInherited.Security)
		assert.Equal(t, []openapi.SecurityRequirement{{"OAuth2": {}}}, *opInherited.Security)
		assert.Equal(t, []string{"OAuth2"}, opInherited.ReferencedSecSchemes)

		strictTrue := true
		opMetaOverride, srMeta := compileOp[overrideReq, simpleRes](t, contract.CompileOptions{
			DefaultStrictJSON:  false,
			StrictJSONOverride: &strictTrue,
			InheritedTags:      []string{"GroupTag"},
			HasInheritedSec:    true,
			InheritedSecurity:  []string{"GroupScheme"},
			HasEndpointTags:    true,
			EndpointTags:       []string{"EndpointTag"},
			HasEndpointSec:     true,
			EndpointSecurity:   []string{"EndpointScheme"},
		})
		assert.Equal(t, []string{"MetaTag"}, opMetaOverride.Tags)
		assert.False(t, opMetaOverride.StrictJSON)
		assert.True(t, opMetaOverride.NullableRes)
		require.NotNil(t, opMetaOverride.Security)
		require.Len(t, *opMetaOverride.Security, 1)
		assert.Equal(t, openapi.SecurityRequirement{"ApiKey": {}, "Bearer": {}}, (*opMetaOverride.Security)[0])
		assert.NotNil(t, (*opMetaOverride.Security)[0]["Bearer"])
		assert.Equal(t, []string{"Bearer", "ApiKey"}, opMetaOverride.ReferencedSecSchemes)
		assert.Equal(t, []int{409, 422}, opMetaOverride.ErrorStatuses)

		oasOp := opMetaOverride.BuildOpenAPIOperation(srMeta)
		assert.Contains(t, oasOp.Responses, "409")
		assert.Contains(t, oasOp.Responses, "422")

		// DocNoSecurity / security:"-"
		opEndpointNoSec, _ := compileOp[baseReq, emptyRes](t, contract.CompileOptions{
			HasInheritedSec:   true,
			InheritedSecurity: []string{"Bearer"},
			HasEndpointSec:    true,
			EndpointSecurity:  []string{},
		})
		require.NotNil(t, opEndpointNoSec.Security)
		assert.Empty(t, *opEndpointNoSec.Security)
		assert.Empty(t, opEndpointNoSec.ReferencedSecSchemes)

		opMetaNoSec, _ := compileOp[noSecReq, emptyRes](t, contract.CompileOptions{
			HasInheritedSec:   true,
			InheritedSecurity: []string{"Bearer"},
		})
		require.NotNil(t, opMetaNoSec.Security)
		assert.Empty(t, *opMetaNoSec.Security)
	})
}

func TestCompileOperation_TagAndValidatorPrechecks(t *testing.T) {
	cases := []struct {
		name string
		req  reflect.Type
	}{
		{
			name: "invalid_default_int",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				Limit   int `query:"limit" default:"abc"`
			})(nil)),
		},
		{
			name: "invalid_default_nan",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				Score   float64 `query:"score" default:"NaN"`
			})(nil)),
		},
		{
			name: "default_on_path",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x/{id}" method:"GET"`
				ID      string `path:"id" default:"123"`
			})(nil)),
		},
		{
			name: "default_with_required",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				Page    int `query:"page" default:"1" validate:"required"`
			})(nil)),
		},
		{
			name: "invalid_example_json_slice",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Tags    []string `json:"tags" example:"[invalid"`
			})(nil)),
		},
		{
			name: "invalid_example_base64_bytes",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Blob    []byte `json:"blob" example:"%%%invalid%%%"`
			})(nil)),
		},
		{
			name: "invalid_validator_rule_name",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Name    string `json:"name" validate:"nonexistent_rule_xyz"`
			})(nil)),
		},
		{
			name: "lazy_omitempty_invalid_validator_param",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Name    string `json:"name" validate:"omitempty,min=notanumber"`
			})(nil)),
		},
		{
			name: "lazy_dive_invalid_validator_param",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Tags    []string `json:"tags" validate:"dive,min=badnum"`
			})(nil)),
		},
		{
			name: "broken_cross_field_ref",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Pass    string `json:"pass" validate:"eqfield=MissingConfirm"`
			})(nil)),
		},
		{
			name: "mismatched_validate_and_binding",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Name    string `json:"name" validate:"required" binding:"min=2"`
			})(nil)),
		},
		{
			name: "unexported_field_with_binding_tag",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				secret  string `query:"secret" validate:"required"`
			})(nil)),
		},
		{
			name: "reserved_header_authorization",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				Auth    string `header:"Authorization"`
			})(nil)),
		},
		{
			name: "reserved_header_content_type",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				CT      string `header:"content-type"`
			})(nil)),
		},
		{
			name: "reserved_header_accept",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				Acc     string `header:"Accept"`
			})(nil)),
		},
		{
			name: "multiple_source_tags_on_field",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"GET"`
				ID      string `query:"id" header:"X-Id"`
			})(nil)),
		},
		{
			name: "source_tag_in_nested_struct",
			req: reflect.TypeOf((*struct {
				pi.Meta `path:"/x" method:"POST"`
				Nested  struct {
					Page int `query:"page"`
				} `json:"nested"`
			})(nil)),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err := contract.CompileOperation("h", tc.req, reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}, contract.NewSchemaRegistry())
				assert.Error(t, err)
			})
		})
	}
}

func TestBindAndValidate_MultiSourceIsolationAndTypes(t *testing.T) {
	type multiReq struct {
		pi.Meta `path:"/orgs/{id}/items" method:"POST"`
		ID      string     `path:"id"`
		Query   string     `query:"q"`
		Flag    bool       `query:"flag" default:"true"`
		Num     int        `query:"n" default:"42"`
		Str     string     `query:"s" default:"fallback"`
		Tags    []string   `query:"tag"`
		When    *time.Time `query:"when"`
		UID     uuid.UUID  `query:"uid"`
		Trace   string     `header:"X-Trace"`
		Name    string     `json:"name"`
		OptNum  *int       `json:"optNum" default:"9"`
	}

	op, _ := compileOp[multiReq, simpleRes](t, contract.CompileOptions{GroupPrefix: "/api"})

	fixedTime := "2026-11-14T10:00:00Z"
	fixedUUID := uuid.New().String()

	t.Run("isolation_slices_and_explicit_zeros", func(t *testing.T) {
		var req multiReq
		// JSON body tries to spoof path/query/header keys without json:"-"
		body := []byte(`{"id":"evil-id","q":"evil-q","x-trace":"evil-trace","name":"alice"}`)
		target := "/api/orgs/org-123/items?q=hello&flag=false&n=0&s=&tag=a&tag=b,c&when=" + fixedTime + "&uid=" + fixedUUID
		headers := http.Header{"X-Trace": []string{"trace-xyz"}}
		piReq := makePIRequestWithHeaders(http.MethodPost, target, body, "application/json", map[string]string{"id": "org-123"}, headers)

		err := op.BindAndValidate(piReq, &req, nil)
		require.NoError(t, err)

		assert.Equal(t, "org-123", req.ID)
		assert.Equal(t, "hello", req.Query)
		assert.Equal(t, "trace-xyz", req.Trace)
		assert.Equal(t, "alice", req.Name)

		// Explicit zero values must NOT be replaced by defaults
		assert.False(t, req.Flag)
		assert.Equal(t, 0, req.Num)
		assert.Equal(t, "", req.Str)

		// Repeated query params preserved, commas NOT split
		assert.Equal(t, []string{"a", "b,c"}, req.Tags)

		require.NotNil(t, req.When)
		assert.Equal(t, fixedTime, req.When.UTC().Format(time.RFC3339))
		assert.Equal(t, fixedUUID, req.UID.String())

		// Omitted optNum uses default
		require.NotNil(t, req.OptNum)
		assert.Equal(t, 9, *req.OptNum)
	})

	t.Run("defaults_when_absent_and_hpp_duplicate_scalar", func(t *testing.T) {
		var reqDefaults multiReq
		piReq := makePIRequest(http.MethodPost, "/api/orgs/org-1/items", []byte(`{"name":"bob"}`), "application/json", map[string]string{"id": "org-1"})
		require.NoError(t, op.BindAndValidate(piReq, &reqDefaults, nil))
		assert.True(t, reqDefaults.Flag)
		assert.Equal(t, 42, reqDefaults.Num)
		assert.Equal(t, "fallback", reqDefaults.Str)

		var reqHPP multiReq
		piHPP := makePIRequest(http.MethodPost, "/api/orgs/org-1/items?n=1&n=2", []byte(`{"name":"bob"}`), "application/json", map[string]string{"id": "org-1"})
		err := op.BindAndValidate(piHPP, &reqHPP, nil)
		require.Error(t, err)
		status, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)
		details := piHTTP.ErrorDetails(err)
		assert.Contains(t, details, apperror.Detail{Field: "n", Rule: "single_value"})

		var reqDupHdr multiReq
		hdrs := http.Header{"X-Trace": []string{"t1", "t2"}}
		piDupHdr := makePIRequestWithHeaders(http.MethodPost, "/api/orgs/org-1/items", []byte(`{"name":"bob"}`), "application/json", map[string]string{"id": "org-1"}, hdrs)
		err = op.BindAndValidate(piDupHdr, &reqDupHdr, nil)
		require.Error(t, err)
		details = piHTTP.ErrorDetails(err)
		assert.Contains(t, details, apperror.Detail{Field: "X-Trace", Rule: "single_value"})
	})
}

func TestBindAndValidate_JSONStrictnessEmptyBodyAndNulls(t *testing.T) {
	t.Run("bodyless_post_contract", func(t *testing.T) {
		type pingReq struct {
			pi.Meta `path:"/ping" method:"POST"`
		}
		op, _ := compileOp[pingReq, emptyRes](t, contract.CompileOptions{})

		var req pingReq
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/ping", nil, "", nil), &req, nil))
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/ping", []byte(`   `), "", nil), &req, nil))
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/ping", []byte(`{}`), "application/json", nil), &req, nil))

		badBodies := []struct {
			name        string
			body        []byte
			contentType string
			wantStatus  int
		}{
			{"non_empty_object", []byte(`{"x":1}`), "application/json", http.StatusBadRequest},
			{"array_body", []byte(`[]`), "application/json", http.StatusBadRequest},
			{"null_body", []byte(`null`), "application/json", http.StatusBadRequest},
			{"trailing_object", []byte(`{}{}`), "application/json", http.StatusBadRequest},
			{"wrong_content_type", []byte(`hello`), "text/plain", http.StatusUnsupportedMediaType},
		}
		for _, tc := range badBodies {
			t.Run(tc.name, func(t *testing.T) {
				err := op.BindAndValidate(makePIRequest(http.MethodPost, "/ping", tc.body, tc.contentType, nil), &req, nil)
				require.Error(t, err)
				status, _, _ := piHTTP.ErrorResponse(err)
				assert.Equal(t, tc.wantStatus, status)
			})
		}
	})

	t.Run("strict_vs_lax_case_dup_null_and_content_type", func(t *testing.T) {
		type nested struct {
			Code string `json:"code"`
		}
		type itemReq struct {
			pi.Meta `path:"/items" method:"POST"`
			Name    string         `json:"name"`
			Age     int            `json:"age"`
			OptAge  *int           `json:"optAge" default:"7"`
			Nested  nested         `json:"nested"`
			MetaMap map[string]any `json:"metaMap"`
		}
		type strictTagReq struct {
			pi.Meta `path:"/strict-items" method:"POST" strict:"true"`
			Name    string `json:"name"`
			Nested  nested `json:"nested"`
		}

		opLax, _ := compileOp[itemReq, simpleRes](t, contract.CompileOptions{DefaultStrictJSON: false})
		opStrict, _ := compileOp[strictTagReq, simpleRes](t, contract.CompileOptions{DefaultStrictJSON: false})

		// Non-JSON Content-Type -> 415; application/*+json -> allowed
		var r0 itemReq
		err := opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", []byte(`{"name":"a"}`), "text/plain", nil), &r0, nil)
		require.Error(t, err)
		status, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusUnsupportedMediaType, status)

		var rPlusJSON itemReq
		err = opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", []byte(`{"name":"a"}`), "application/problem+json; charset=utf-8", nil), &rPlusJSON, nil)
		require.NoError(t, err)
		assert.Equal(t, "a", rPlusJSON.Name)

		// Case sensitivity: "Name" does NOT populate "name"
		var rCase itemReq
		err = opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", []byte(`{"Name":"Wrong","extra":1}`), "application/json", nil), &rCase, nil)
		require.NoError(t, err)
		assert.Equal(t, "", rCase.Name)

		// Strict mode rejects unknown top-level & nested unknown keys and mismatched case key
		var rStrict strictTagReq
		err = opStrict.BindAndValidate(makePIRequest(http.MethodPost, "/strict-items", []byte(`{"Name":"a","nested":{"code":"c","extra":true}}`), "application/json", nil), &rStrict, nil)
		require.Error(t, err)
		status, _, _ = piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)
		details := piHTTP.ErrorDetails(err)
		assert.Contains(t, details, apperror.Detail{Field: "Name", Rule: "unknown_field"})
		assert.Contains(t, details, apperror.Detail{Field: "nested.extra", Rule: "unknown_field"})

		// Duplicate keys rejected (top-level, nested struct, map)
		for _, payload := range [][]byte{
			[]byte(`{"name":"a","name":"b"}`),
			[]byte(`{"nested":{"code":"a","code":"b"}}`),
			[]byte(`{"metaMap":{"k":1,"k":2}}`),
		} {
			var rDup itemReq
			err := opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", payload, "application/json", nil), &rDup, nil)
			require.Error(t, err)
			details := piHTTP.ErrorDetails(err)
			require.NotEmpty(t, details)
			assert.Equal(t, "duplicate_key", details[0].Rule)
		}

		// Trailing tokens rejected
		var rTrail itemReq
		err = opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", []byte(`{"name":"a"}{}`), "application/json", nil), &rTrail, nil)
		require.Error(t, err)
		status, _, _ = piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)

		// Explicit null on non-pointer scalar rejected; explicit null on pointer does NOT apply default
		var rNullNonPtr itemReq
		err = opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", []byte(`{"age":null}`), "application/json", nil), &rNullNonPtr, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "age", Rule: "type"})

		var rNullPtr itemReq
		err = opLax.BindAndValidate(makePIRequest(http.MethodPost, "/items", []byte(`{"optAge":null}`), "application/json", nil), &rNullPtr, nil)
		require.NoError(t, err)
		assert.Nil(t, rNullPtr.OptAge)
	})
}

func TestBindAndValidate_ValidationSanitizationAndCustomValidator(t *testing.T) {
	t.Run("sorted_deduped_capped_details_and_nested_path", func(t *testing.T) {
		type orderItem struct {
			Price int `json:"price" validate:"gt=0"`
		}
		type bulkReq struct {
			pi.Meta `path:"/orders" method:"POST"`
			Items   []orderItem `json:"items" validate:"required,dive"`
		}

		op, _ := compileOp[bulkReq, simpleRes](t, contract.CompileOptions{})

		var b strings.Builder
		b.WriteString(`{"items":[`)
		for i := 0; i < 40; i++ {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(`{"price":0}`)
		}
		b.WriteString(`]}`)

		var req bulkReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/orders", []byte(b.String()), "application/json", nil), &req, nil)
		require.Error(t, err)
		status, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)

		details := piHTTP.ErrorDetails(err)
		require.Len(t, details, 32)
		assert.Equal(t, apperror.Detail{Field: "items[0].price", Rule: "gt"}, details[0])
		assert.Equal(t, apperror.Detail{Field: "items[10].price", Rule: "gt"}, details[1])
		assert.Contains(t, details, apperror.Detail{Field: "items[1].price", Rule: "gt"})

		// Verify strict lexicographical sort & uniqueness
		for i := 1; i < len(details); i++ {
			prev, cur := details[i-1], details[i]
			assert.True(t, prev.Field < cur.Field || (prev.Field == cur.Field && prev.Rule < cur.Rule))
		}

		// Verify exact nested path when only second element fails
		var singleReq bulkReq
		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/orders", []byte(`{"items":[{"price":10},{"price":0}]}`), "application/json", nil), &singleReq, nil)
		require.Error(t, err)
		assert.Equal(t, []apperror.Detail{{Field: "items[1].price", Rule: "gt"}}, piHTTP.ErrorDetails(err))
	})

	t.Run("cross_source_collision_prefix_and_secret_redaction", func(t *testing.T) {
		type collisionReq struct {
			pi.Meta  `path:"/users/{userId}" method:"POST"`
			PathID   string `path:"userId"`
			QueryID  string `query:"userId" validate:"min=10"`
			BodyID   string `json:"userId"`
			Password string `json:"password" validate:"min=30,oneof=expectedToken"`
		}

		op, _ := compileOp[collisionReq, simpleRes](t, contract.CompileOptions{})

		secret := "SuperSecretPass!999"
		var req collisionReq
		piReq := makePIRequest(
			http.MethodPost,
			"/users/u1?userId=short",
			[]byte(`{"userId":"b1","password":"`+secret+`"}`),
			"application/json",
			map[string]string{"userId": "u1"},
		)
		err := op.BindAndValidate(piReq, &req, nil)
		require.Error(t, err)

		var appErr *apperror.Error
		require.ErrorAs(t, err, &appErr)
		assert.Equal(t, apperror.InvalidArgument, appErr.Kind())
		assert.NotContains(t, err.Error(), secret)
		assert.NotContains(t, appErr.PublicMessage(), secret)

		status, _, publicMsg := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.NotContains(t, publicMsg, secret)

		details := piHTTP.ErrorDetails(err)
		assert.Contains(t, details, apperror.Detail{Field: "query.userId", Rule: "min"})
		assert.Contains(t, details, apperror.Detail{Field: "password", Rule: "min"})
		for _, d := range details {
			assert.NotContains(t, d.Field, secret)
			assert.NotContains(t, d.Rule, secret)
		}
	})

	t.Run("custom_validator_invocation_and_sanitization", func(t *testing.T) {
		type authReq struct {
			pi.Meta `path:"/login" method:"POST"`
			Secret  string `json:"secret" validate:"required"`
		}
		op, _ := compileOp[authReq, simpleRes](t, contract.CompileOptions{})

		secret := "RawLeakValue!12345"
		domainValidator := piHTTP.NewValidator("en")
		calls := 0
		customFn := func(v any) error {
			calls++
			req := v.(*authReq)
			type secretCheck struct {
				Secret string `json:"secret" binding:"min=30" msg:"bad secret: {value}"`
			}
			return domainValidator(&secretCheck{Secret: req.Secret})
		}

		var req authReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/login", []byte(`{"secret":"`+secret+`"}`), "application/json", nil), &req, customFn)
		require.Error(t, err)
		assert.Equal(t, 1, calls)

		var appErr *apperror.Error
		require.ErrorAs(t, err, &appErr)
		assert.Equal(t, apperror.InvalidArgument, appErr.Kind())
		assert.NotContains(t, err.Error(), secret)
		assert.NotContains(t, appErr.PublicMessage(), secret)
		assert.Contains(t, appErr.Details(), apperror.Detail{Field: "secret", Rule: "min"})

		// Custom business error preserved untouched
		bizErr := apperror.New(apperror.Conflict, http.StatusConflict, "duplicate entry")
		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/login", []byte(`{"secret":"ok"}`), "application/json", nil), &req, func(any) error {
			return bizErr
		})
		assert.Equal(t, bizErr, err)
	})
}

func TestCompiledOperation_WrapResponse(t *testing.T) {
	type post201Req struct {
		pi.Meta `path:"/users" method:"POST" status:"201"`
	}
	type put204Req struct {
		pi.Meta `path:"/users" method:"PUT" status:"204"`
	}
	type getNullableReq struct {
		pi.Meta `path:"/user" method:"GET" nullable:"true"`
	}
	type getStrictReq struct {
		pi.Meta `path:"/user" method:"GET"`
	}

	op201, _ := compileOp[post201Req, simpleRes](t, contract.CompileOptions{})
	wrapped, err := op201.WrapResponse(&simpleRes{OK: true}, nil)
	require.NoError(t, err)
	assert.Equal(t, resTypes.Result{StatusCode: http.StatusCreated, Data: &simpleRes{OK: true}}, wrapped)

	// Untyped or typed nil on non-nullable non-empty Res -> 500 error
	_, err = op201.WrapResponse((*simpleRes)(nil), nil)
	require.Error(t, err)
	status, _, _ := piHTTP.ErrorResponse(err)
	assert.Equal(t, http.StatusInternalServerError, status)

	_, err = op201.WrapResponse(nil, nil)
	require.Error(t, err)

	// Error takes precedence over response
	errBoom := apperror.New(apperror.NotFound, http.StatusNotFound, "not found")
	resOut, err := op201.WrapResponse(&simpleRes{OK: true}, errBoom)
	assert.Nil(t, resOut)
	assert.Equal(t, errBoom, err)

	// 204 returns resTypes.NoContent()
	op204, _ := compileOp[put204Req, emptyRes](t, contract.CompileOptions{})
	wrapped204, err := op204.WrapResponse(&emptyRes{}, nil)
	require.NoError(t, err)
	assert.Equal(t, resTypes.NoContent(), wrapped204)

	// Empty Res with 200 -> data: nil even if non-nil pointer passed
	opEmpty200, _ := compileOp[getStrictReq, emptyRes](t, contract.CompileOptions{})
	wrappedEmpty, err := opEmpty200.WrapResponse(&emptyRes{}, nil)
	require.NoError(t, err)
	assert.Equal(t, resTypes.Result{StatusCode: http.StatusOK, Data: nil}, wrappedEmpty)

	// Nullable non-empty Res returning typed nil -> data: nil
	opNullable, _ := compileOp[getNullableReq, simpleRes](t, contract.CompileOptions{})
	wrappedNullable, err := opNullable.WrapResponse((*simpleRes)(nil), nil)
	require.NoError(t, err)
	assert.Equal(t, resTypes.Result{StatusCode: http.StatusOK, Data: nil}, wrappedNullable)
}

func TestBuildOpenAPIOperation_SchemasGenericsAndCycles(t *testing.T) {
	t.Run("special_types_json_string_nullable_and_envelopes", func(t *testing.T) {
		type schemaReq struct {
			pi.Meta   `path:"/assets" method:"POST" summary:"Create asset" strict:"true"`
			Payload   []byte         `json:"payload"`
			CreatedAt time.Time      `json:"createdAt"`
			RequestID uuid.UUID      `json:"requestId"`
			CountStr  int64          `json:"countStr,string" default:"42" example:"99"`
			Child     *nullableChild `json:"child" dc:"optional child"`
		}
		type assetRes struct {
			ID string `json:"id"`
		}

		op, compileSR := compileOp[schemaReq, assetRes](t, contract.CompileOptions{})
		exportSR := contract.NewSchemaRegistry()
		oasOp := op.BuildOpenAPIOperation(exportSR)
		require.NotNil(t, oasOp)
		require.NotNil(t, oasOp.RequestBody)

		// Both compile-time and export-time registries hold identical component schema
		schemas := exportSR.Schemas()
		require.Equal(t, compileSR.Schemas(), schemas)

		reqSchemaRef := oasOp.RequestBody.Content["application/json"].Schema
		require.NotNil(t, reqSchemaRef)
		assert.Equal(t, "#/components/schemas/schemaReq", reqSchemaRef.Ref)

		reqObj := schemas["schemaReq"]
		require.NotNil(t, reqObj)
		require.NotNil(t, reqObj.AdditionalProperties)
		require.NotNil(t, reqObj.AdditionalProperties.Allowed)
		assert.False(t, *reqObj.AdditionalProperties.Allowed)

		assert.Equal(t, "string", reqObj.Properties["payload"].Type)
		assert.Equal(t, "byte", reqObj.Properties["payload"].Format)
		assert.Equal(t, "string", reqObj.Properties["createdAt"].Type)
		assert.Equal(t, "date-time", reqObj.Properties["createdAt"].Format)
		assert.Equal(t, "string", reqObj.Properties["requestId"].Type)
		assert.Equal(t, "uuid", reqObj.Properties["requestId"].Format)

		countProp := reqObj.Properties["countStr"]
		assert.Equal(t, "string", countProp.Type)
		assert.Equal(t, "42", countProp.Default)
		assert.Equal(t, "99", countProp.Example)

		// OAS 3.0.3 nullable $ref via allOf
		childProp := reqObj.Properties["child"]
		assert.Empty(t, childProp.Ref)
		assert.True(t, childProp.Nullable)
		assert.Equal(t, "optional child", childProp.Description)
		require.Len(t, childProp.AllOf, 1)
		assert.Equal(t, "#/components/schemas/nullableChild", childProp.AllOf[0].Ref)

		// Envelope + 415 documented on POST
		resp200 := oasOp.Responses["200"]
		require.NotNil(t, resp200)
		envSchema := resp200.Content["application/json"].Schema
		assert.Equal(t, []string{"code", "data", "message"}, envSchema.Required)
		assert.Equal(t, "#/components/schemas/assetRes", envSchema.Properties["data"].Ref)
		assert.Contains(t, oasOp.Responses, "400")
		assert.Contains(t, oasOp.Responses, "408")
		assert.Contains(t, oasOp.Responses, "413")
		assert.Contains(t, oasOp.Responses, "415")
		assert.Contains(t, oasOp.Responses, "500")
	})

	t.Run("nullable_response_allof_and_bodyless_415_vs_get", func(t *testing.T) {
		type nullableResReq struct {
			pi.Meta `path:"/item" method:"GET" nullable:"true"`
		}
		type bodylessPostReq struct {
			pi.Meta `path:"/ping" method:"POST"`
		}
		type itemData struct {
			ID string `json:"id"`
		}

		opNullRes, sr1 := compileOp[nullableResReq, itemData](t, contract.CompileOptions{})
		oasNull := opNullRes.BuildOpenAPIOperation(sr1)
		dataSchema := oasNull.Responses["200"].Content["application/json"].Schema.Properties["data"]
		assert.Empty(t, dataSchema.Ref)
		assert.True(t, dataSchema.Nullable)
		require.Len(t, dataSchema.AllOf, 1)
		assert.Equal(t, "#/components/schemas/itemData", dataSchema.AllOf[0].Ref)
		assert.NotContains(t, oasNull.Responses, "415")

		opPing, sr2 := compileOp[bodylessPostReq, emptyRes](t, contract.CompileOptions{})
		oasPing := opPing.BuildOpenAPIOperation(sr2)
		assert.Contains(t, oasPing.Responses, "415")
	})

	t.Run("cyclic_struct_generics_and_cross_package_collision", func(t *testing.T) {
		type treeReq struct {
			pi.Meta `path:"/tree" method:"GET"`
		}
		opTree, srTree := compileOp[treeReq, treeNodeRes](t, contract.CompileOptions{})
		oasTree := opTree.BuildOpenAPIOperation(srTree)
		require.NotNil(t, oasTree)
		treeSchema := srTree.Schemas()["treeNodeRes"]
		require.NotNil(t, treeSchema)
		nextProp := treeSchema.Properties["next"]
		require.NotNil(t, nextProp)
		assert.True(t, nextProp.Nullable)
		require.Len(t, nextProp.AllOf, 1)
		assert.Equal(t, "#/components/schemas/treeNodeRes", nextProp.AllOf[0].Ref)

		// Generics + cross-package short-name collision
		type pageReq struct {
			pi.Meta `path:"/pages" method:"GET"`
		}
		type slicePageReq struct {
			pi.Meta `path:"/slice-pages" method:"GET"`
		}
		type collisionRes struct {
			StdTag   openapi.Tag `json:"stdTag"`
			LocalTag Tag         `json:"localTag"`
		}
		type collisionReq struct {
			pi.Meta `path:"/tags" method:"GET"`
		}

		sharedSR := contract.NewSchemaRegistry()
		opPage1, err := contract.CompileOperation("p1", reflect.TypeOf((*pageReq)(nil)), reflect.TypeOf((*PageRes[UserItem])(nil)), contract.CompileOptions{}, sharedSR)
		require.NoError(t, err)
		opPage2, err := contract.CompileOperation("p2", reflect.TypeOf((*slicePageReq)(nil)), reflect.TypeOf((*PageRes[[]UserItem])(nil)), contract.CompileOptions{}, sharedSR)
		require.NoError(t, err)
		opColl, err := contract.CompileOperation("c1", reflect.TypeOf((*collisionReq)(nil)), reflect.TypeOf((*collisionRes)(nil)), contract.CompileOptions{}, sharedSR)
		require.NoError(t, err)

		exportSR := contract.NewSchemaRegistry()
		_ = opPage1.BuildOpenAPIOperation(exportSR)
		_ = opPage2.BuildOpenAPIOperation(exportSR)
		oasColl := opColl.BuildOpenAPIOperation(exportSR)

		schemas := exportSR.Schemas()
		assert.Contains(t, schemas, "PageRes_UserItem")
		assert.Contains(t, schemas, "PageRes_List_UserItem")

		collSchema := schemas["collisionRes"]
		require.NotNil(t, collSchema)
		stdRef := collSchema.Properties["stdTag"].Ref
		localRef := collSchema.Properties["localTag"].Ref
		assert.Equal(t, "#/components/schemas/Tag", stdRef)
		assert.True(t, strings.HasPrefix(localRef, "#/components/schemas/Tag_"))
		assert.NotEqual(t, stdRef, localRef)

		localCompName := strings.TrimPrefix(localRef, "#/components/schemas/")
		assert.Contains(t, schemas["Tag"].Properties, "name")
		assert.Contains(t, schemas[localCompName].Properties, "label")

		// Idempotent second call on same registry
		oasColl2 := opColl.BuildOpenAPIOperation(exportSR)
		assert.Equal(t, oasColl.OperationID, oasColl2.OperationID)
		assert.Len(t, exportSR.Schemas(), len(schemas))
	})
}

func TestNormalizeConflictAndOpenAPIPaths(t *testing.T) {
	// Distinct regex constraints remain distinct for route conflict detection, while identical regexes collide.
	assert.Equal(t, "/users/{:[0-9]+}", contract.NormalizeConflictPath("/users/{id:[0-9]+}"))
	assert.Equal(t, "/users/{:[a-z]+}", contract.NormalizeConflictPath("/users/{slug:[a-z]+}"))
	assert.Equal(t,
		contract.NormalizeConflictPath("/users/{id:[0-9]+}"),
		contract.NormalizeConflictPath("/users/{uid:[0-9]+}"),
	)
	assert.NotEqual(t,
		contract.NormalizeConflictPath("/users/{id:[0-9]+}"),
		contract.NormalizeConflictPath("/users/{slug:[a-z]+}"),
	)

	// Nested regex quantifiers {m,n} are matched via brace-depth counting.
	assert.Equal(t, "/items/{:[0-9]{1,3}}", contract.NormalizeConflictPath("/items/{id:[0-9]{1,3}}"))
	assert.Equal(t, "/orgs/{}/users/{:[a-f0-9]{8}}", contract.NormalizeConflictPath("/orgs/{orgId}/users/{id:[a-f0-9]{8}}"))
	assert.Equal(t, "/users/{}", contract.NormalizeConflictPath("/users/{id}"))
	assert.Equal(t, "/broken/{unclosed", contract.NormalizeConflictPath("/broken/{unclosed"))

	// OpenAPI path template normalization strips all parameter names and regex constraints uniformly.
	assert.Equal(t, "/users/{}", contract.NormalizeOpenAPIPathTemplate("/users/{id}"))
	assert.Equal(t, "/users/{}", contract.NormalizeOpenAPIPathTemplate("/users/{name}"))
	assert.Equal(t, "/users/{}/posts/{}", contract.NormalizeOpenAPIPathTemplate("/users/{id:[0-9]{1,3}}/posts/{slug:[a-z]+}"))
	assert.Equal(t, "/broken/{unclosed", contract.NormalizeOpenAPIPathTemplate("/broken/{unclosed"))

	// NormalizeRouteConflict delegates canonical path normalization cleanly.
	canonID, shapeID := contract.NormalizeRouteConflict("/api/v1", http.MethodGet, "/users/{id:[0-9]+}")
	canonSlug, shapeSlug := contract.NormalizeRouteConflict("/api/v1", http.MethodGet, "/users/{slug:[a-z]+}")
	canonUID, shapeUID := contract.NormalizeRouteConflict("/api/v1", http.MethodGet, "/users/{uid:[0-9]+}")
	assert.Equal(t, "/api/v1/users/{id:[0-9]+}", canonID)
	assert.Equal(t, "/api/v1/users/{slug:[a-z]+}", canonSlug)
	assert.Equal(t, "/api/v1/users/{uid:[0-9]+}", canonUID)
	assert.Equal(t, "GET /api/v1/users/{:[0-9]+}", shapeID)
	assert.Equal(t, "GET /api/v1/users/{:[a-z]+}", shapeSlug)
	assert.NotEqual(t, shapeID, shapeSlug)
	assert.Equal(t, shapeID, shapeUID)
}

func TestSchemaNullability_OutputsAndTypelessSchemas(t *testing.T) {
	type nullabilityPayload struct {
		Slice  []int             `json:"slice"`
		Bytes  []byte            `json:"bytes"`
		Map    map[string]string `json:"map"`
		Arr    [2]int            `json:"arr"`
		PtrArr *[2]int           `json:"ptrArr"`
		Raw    json.RawMessage   `json:"raw"`
		PtrRaw *json.RawMessage  `json:"ptrRaw"`
		Dyn    any               `json:"dyn"`
		PtrDyn *any              `json:"ptrDyn"`
	}
	type nullabilityReq struct {
		pi.Meta `path:"/nullability" method:"POST" strict:"true"`
		Payload nullabilityPayload `json:"payload"`
	}

	op, sr := compileOp[nullabilityReq, nullabilityPayload](t, contract.CompileOptions{})
	oasOp := op.BuildOpenAPIOperation(sr)
	schemas := sr.Schemas()

	reqSchemaRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
	reqObj := schemas[strings.TrimPrefix(reqSchemaRef, "#/components/schemas/")]
	inPayloadRef := reqObj.Properties["payload"].Ref
	inPayload := schemas[strings.TrimPrefix(inPayloadRef, "#/components/schemas/")]

	outDataRef := oasOp.Responses["200"].Content["application/json"].Schema.Properties["data"].Ref
	outPayload := schemas[strings.TrimPrefix(outDataRef, "#/components/schemas/")]

	// Input slices/bytes/maps remain non-nullable unless pointer; outputs become nullable because Go nil marshals as null.
	assert.False(t, inPayload.Properties["slice"].Nullable)
	assert.False(t, inPayload.Properties["bytes"].Nullable)
	assert.False(t, inPayload.Properties["map"].Nullable)
	assert.False(t, inPayload.Properties["arr"].Nullable)
	assert.True(t, inPayload.Properties["ptrArr"].Nullable)

	assert.True(t, outPayload.Properties["slice"].Nullable)
	assert.True(t, outPayload.Properties["bytes"].Nullable)
	assert.True(t, outPayload.Properties["map"].Nullable)
	assert.False(t, outPayload.Properties["arr"].Nullable)
	assert.True(t, outPayload.Properties["ptrArr"].Nullable)

	// OAS 3.0.3 forbids nullable:true on typeless {} schemas (json.RawMessage / any, even behind pointers).
	for _, propName := range []string{"raw", "ptrRaw", "dyn", "ptrDyn"} {
		inProp := inPayload.Properties[propName]
		assert.Empty(t, inProp.Type, "input %s should be typeless {}", propName)
		assert.False(t, inProp.Nullable, "input %s must not set nullable:true without type", propName)

		outProp := outPayload.Properties[propName]
		assert.Empty(t, outProp.Type, "output %s should be typeless {}", propName)
		assert.False(t, outProp.Nullable, "output %s must not set nullable:true without type", propName)
	}
}

func TestCompileOperation_PerRuleValidatorPrecheck(t *testing.T) {
	t.Run("rejects_short_circuited_and_malformed_rules_without_panic", func(t *testing.T) {
		type seqBadReq struct {
			pi.Meta `path:"/seq" method:"POST"`
			State   string `json:"state" validate:"eq=ready,min=oops"`
		}
		type orBadReq struct {
			pi.Meta `path:"/or" method:"POST"`
			State   string `json:"state" validate:"eq=ready|min=oops"`
		}
		type diveNonIterableReq struct {
			pi.Meta `path:"/dive-str" method:"POST"`
			Name    string `json:"name" validate:"dive,required"`
		}
		type keysWithoutDiveReq struct {
			pi.Meta `path:"/keys-no-dive" method:"POST"`
			Attrs   map[string]int `json:"attrs" validate:"keys,min=1,endkeys"`
		}
		type unclosedKeysReq struct {
			pi.Meta `path:"/unclosed-keys" method:"POST"`
			Attrs   map[string]int `json:"attrs" validate:"dive,keys,min=1"`
		}
		type endkeysWithoutKeysReq struct {
			pi.Meta `path:"/endkeys-no-keys" method:"POST"`
			Attrs   map[string]int `json:"attrs" validate:"dive,endkeys"`
		}
		type badMapKeyRuleReq struct {
			pi.Meta `path:"/bad-map-key" method:"POST"`
			Attrs   map[string]int `json:"attrs" validate:"dive,keys,min=oops,endkeys"`
		}
		type oddRequiredIfReq struct {
			pi.Meta `path:"/odd-req-if" method:"POST"`
			Mode    string `json:"mode"`
			Target  string `json:"target" validate:"eq=ready,required_if=Mode"`
		}
		type diveSiblingCrossFieldReq struct {
			pi.Meta `path:"/dive-eqfield" method:"POST"`
			Other   string   `json:"other"`
			Items   []string `json:"items" validate:"dive,eqfield=Other"`
		}
		type missingEqCrossFieldReq struct {
			pi.Meta `path:"/missing-eq" method:"POST"`
			Other   string `json:"other"`
			Target  string `json:"target" validate:"eqfield"`
		}
		type nestedBadItem struct {
			Val string `json:"val" validate:"eq=ready,min=oops"`
		}
		type nestedSliceBadReq struct {
			pi.Meta `path:"/nested-bad" method:"POST"`
			Items   []nestedBadItem `json:"items"`
		}
		type bindingBadReq struct {
			pi.Meta `path:"/binding-bad" method:"POST"`
			State   string `json:"state" binding:"eq=ready,min=oops"`
		}

		invalidReqs := []struct {
			name    string
			reqType reflect.Type
		}{
			{"short_circuited_sequential", reflect.TypeOf((*seqBadReq)(nil))},
			{"short_circuited_or_branch", reflect.TypeOf((*orBadReq)(nil))},
			{"dive_non_iterable", reflect.TypeOf((*diveNonIterableReq)(nil))},
			{"keys_without_dive", reflect.TypeOf((*keysWithoutDiveReq)(nil))},
			{"unclosed_keys", reflect.TypeOf((*unclosedKeysReq)(nil))},
			{"endkeys_without_keys", reflect.TypeOf((*endkeysWithoutKeysReq)(nil))},
			{"bad_map_key_rule", reflect.TypeOf((*badMapKeyRuleReq)(nil))},
			{"odd_required_if_pairs", reflect.TypeOf((*oddRequiredIfReq)(nil))},
			{"dive_sibling_cross_field", reflect.TypeOf((*diveSiblingCrossFieldReq)(nil))},
			{"missing_eq_cross_field", reflect.TypeOf((*missingEqCrossFieldReq)(nil))},
			{"nested_struct_short_circuit", reflect.TypeOf((*nestedSliceBadReq)(nil))},
			{"binding_fallback_short_circuit", reflect.TypeOf((*bindingBadReq)(nil))},
		}

		for _, tc := range invalidReqs {
			t.Run(tc.name, func(t *testing.T) {
				_, err := contract.CompileOperation(tc.name, tc.reqType, reflect.TypeOf((*simpleRes)(nil)), contract.CompileOptions{}, nil)
				require.Error(t, err)
			})
		}
	})

	t.Run("accepts_quoted_cross_field_and_valid_map_dive", func(t *testing.T) {
		type validComplexReq struct {
			pi.Meta `path:"/valid-complex" method:"POST"`
			Mode    string         `json:"mode"`
			Target  string         `json:"target" validate:"required_if=Mode 'foo bar'"`
			Attrs   map[string]int `json:"attrs" validate:"dive,keys,min=1,endkeys,gt=0"`
			State   string         `json:"state" validate:"omitempty,eq=ready|eq=done"`
			Ignored string         `json:"ignored" validate:"-"`
		}

		op, _ := compileOp[validComplexReq, simpleRes](t, contract.CompileOptions{})

		var badReq validComplexReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/valid-complex", []byte(`{"mode":"foo bar","attrs":{"k":1},"state":"ready"}`), "application/json", nil), &badReq, nil)
		require.Error(t, err)
		status, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)

		var okReq validComplexReq
		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/valid-complex", []byte(`{"mode":"foo bar","target":"ok","attrs":{"k":1},"state":"done"}`), "application/json", nil), &okReq, nil)
		require.NoError(t, err)
	})
}

type customJSONSliceCodec []int

func (customJSONSliceCodec) MarshalJSON() ([]byte, error) {
	return []byte(`"custom"`), nil
}

type customJSONMapCodec map[string]int

func (*customJSONMapCodec) UnmarshalJSON(data []byte) error {
	return nil
}

type customTextScalarCodec string

func (c customTextScalarCodec) MarshalText() ([]byte, error) {
	return []byte(c), nil
}

type customTextPtrCodec string

func (c *customTextPtrCodec) UnmarshalText(data []byte) error {
	*c = customTextPtrCodec(data)
	return nil
}

type customMapKeyCodec string

func (k customMapKeyCodec) MarshalText() ([]byte, error) {
	return []byte(k), nil
}

type customJSONResStruct struct {
	ID string `json:"id"`
}

func (customJSONResStruct) MarshalJSON() ([]byte, error) {
	return []byte(`{"id":"custom"}`), nil
}

func TestCompileOperation_RejectsCustomCodecsEverywhere(t *testing.T) {
	t.Run("rejects_json_and_text_codecs_on_all_positions", func(t *testing.T) {
		type sliceReq struct {
			pi.Meta `path:"/c-slice" method:"POST"`
			Items   customJSONSliceCodec `json:"items"`
		}
		type mapReq struct {
			pi.Meta `path:"/c-map" method:"POST"`
			Attrs   customJSONMapCodec `json:"attrs"`
		}
		type textBodyReq struct {
			pi.Meta `path:"/c-text-body" method:"POST"`
			Code    customTextScalarCodec `json:"code"`
		}
		type queryScalarReq struct {
			pi.Meta `path:"/c-query" method:"GET"`
			Code    customTextScalarCodec `query:"code"`
		}
		type querySliceReq struct {
			pi.Meta `path:"/c-query-slice" method:"GET"`
			Codes   []customTextScalarCodec `query:"codes"`
		}
		type pathCodecReq struct {
			pi.Meta `path:"/c-path/{id}" method:"GET"`
			ID      customTextPtrCodec `path:"id"`
		}
		type mapKeyReq struct {
			pi.Meta `path:"/c-map-key" method:"POST"`
			Scores  map[customMapKeyCodec]int `json:"scores"`
		}
		type topResReq struct {
			pi.Meta `path:"/c-res" method:"GET"`
		}
		type nestedResField struct {
			Item customJSONSliceCodec `json:"item"`
		}

		cases := []struct {
			name    string
			reqType reflect.Type
			resType reflect.Type
		}{
			{"named_slice_marshaler", reflect.TypeOf((*sliceReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"named_map_unmarshaler", reflect.TypeOf((*mapReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"text_marshaler_body", reflect.TypeOf((*textBodyReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"text_marshaler_query", reflect.TypeOf((*queryScalarReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"text_marshaler_query_slice", reflect.TypeOf((*querySliceReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"text_unmarshaler_path", reflect.TypeOf((*pathCodecReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"map_key_text_marshaler", reflect.TypeOf((*mapKeyReq)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{"top_level_res_marshaler", reflect.TypeOf((*topResReq)(nil)), reflect.TypeOf((*customJSONResStruct)(nil))},
			{"nested_res_slice_marshaler", reflect.TypeOf((*topResReq)(nil)), reflect.TypeOf((*nestedResField)(nil))},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := contract.CompileOperation(tc.name, tc.reqType, tc.resType, contract.CompileOptions{}, nil)
				require.Error(t, err)
			})
		}
	})

	t.Run("preserves_exempt_standard_types", func(t *testing.T) {
		type standardTypesReq struct {
			pi.Meta `path:"/std" method:"POST"`
			At      time.Time        `json:"at"`
			PtrAt   *time.Time       `json:"ptrAt"`
			ID      uuid.UUID        `json:"id"`
			PtrID   *uuid.UUID       `json:"ptrId"`
			Raw     json.RawMessage  `json:"raw"`
			PtrRaw  *json.RawMessage `json:"ptrRaw"`
			QueryAt *time.Time       `query:"qAt"`
			QueryID uuid.UUID        `query:"qId"`
		}
		type standardTypesRes struct {
			At  time.Time       `json:"at"`
			ID  uuid.UUID       `json:"id"`
			Raw json.RawMessage `json:"raw"`
		}

		_, _ = compileOp[standardTypesReq, standardTypesRes](t, contract.CompileOptions{})
	})
}

func TestBindAndValidate_FixedArrayExactLength(t *testing.T) {
	type fixedArrayReq struct {
		pi.Meta `path:"/coords" method:"POST"`
		Coords  [2]int `json:"coords"`
	}

	op, sr := compileOp[fixedArrayReq, simpleRes](t, contract.CompileOptions{})
	oasOp := op.BuildOpenAPIOperation(sr)
	reqRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
	reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]
	coordsProp := reqObj.Properties["coords"]
	require.NotNil(t, coordsProp.MinItems)
	require.NotNil(t, coordsProp.MaxItems)
	assert.Equal(t, 2, *coordsProp.MinItems)
	assert.Equal(t, 2, *coordsProp.MaxItems)

	var okReq fixedArrayReq
	err := op.BindAndValidate(makePIRequest(http.MethodPost, "/coords", []byte(`{"coords":[1,2]}`), "application/json", nil), &okReq, nil)
	require.NoError(t, err)
	assert.Equal(t, [2]int{1, 2}, okReq.Coords)

	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty_array", `{"coords":[]}`},
		{"too_short", `{"coords":[1]}`},
		{"too_long", `{"coords":[1,2,3]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var badReq fixedArrayReq
			err := op.BindAndValidate(makePIRequest(http.MethodPost, "/coords", []byte(tc.body), "application/json", nil), &badReq, nil)
			require.Error(t, err)
			status, _, _ := piHTTP.ErrorResponse(err)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "coords", Rule: "len"})
		})
	}
}

func TestDiveRequired_ScopingAndSchema(t *testing.T) {
	t.Run("dive_required_only_constrains_elements_not_container_or_body", func(t *testing.T) {
		type diveOnlyReq struct {
			pi.Meta `path:"/dive-only" method:"POST"`
			Items   []string `json:"items" validate:"dive,required"`
			Tags    []string `query:"tags" validate:"dive,required"`
			Blobs   [][]byte `json:"blobs" validate:"dive,required"`
		}

		op, sr := compileOp[diveOnlyReq, simpleRes](t, contract.CompileOptions{})
		oasOp := op.BuildOpenAPIOperation(sr)

		// RequestBody and container field "items" must NOT be required.
		require.NotNil(t, oasOp.RequestBody)
		assert.False(t, oasOp.RequestBody.Required)

		reqRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
		reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]
		assert.NotContains(t, reqObj.Required, "items")
		assert.NotContains(t, reqObj.Required, "blobs")

		// String element gets minLength: 1; []byte element (format: byte) does not.
		itemsSchema := reqObj.Properties["items"]
		require.NotNil(t, itemsSchema.Items)
		require.NotNil(t, itemsSchema.Items.MinLength)
		assert.Equal(t, 1, *itemsSchema.Items.MinLength)

		blobsSchema := reqObj.Properties["blobs"]
		require.NotNil(t, blobsSchema.Items)
		assert.Equal(t, "byte", blobsSchema.Items.Format)
		assert.Nil(t, blobsSchema.Items.MinLength)

		// Query slice with dive,required is optional parameter with minLength: 1 on items.
		require.Len(t, oasOp.Parameters, 1)
		tagsParam := oasOp.Parameters[0]
		assert.Equal(t, "tags", tagsParam.Name)
		assert.False(t, tagsParam.Required)
		require.NotNil(t, tagsParam.Schema.Items)
		require.NotNil(t, tagsParam.Schema.Items.MinLength)
		assert.Equal(t, 1, *tagsParam.Schema.Items.MinLength)

		// Runtime accepts omitted items and empty body, and non-empty items, while rejecting empty string elements.
		var emptyReq diveOnlyReq
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/dive-only", nil, "", nil), &emptyReq, nil))
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/dive-only", []byte(`{}`), "application/json", nil), &emptyReq, nil))

		var okReq diveOnlyReq
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/dive-only", []byte(`{"items":["ok"]}`), "application/json", nil), &okReq, nil))
		assert.Equal(t, []string{"ok"}, okReq.Items)

		var badElemReq diveOnlyReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/dive-only", []byte(`{"items":[""]}`), "application/json", nil), &badElemReq, nil)
		require.Error(t, err)
		status, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)
	})

	t.Run("required_before_dive_keeps_container_and_body_required", func(t *testing.T) {
		type reqDiveReq struct {
			pi.Meta `path:"/req-dive" method:"POST"`
			Items   []string `json:"items" validate:"required,dive,required"`
		}

		op, sr := compileOp[reqDiveReq, simpleRes](t, contract.CompileOptions{})
		oasOp := op.BuildOpenAPIOperation(sr)

		require.NotNil(t, oasOp.RequestBody)
		assert.True(t, oasOp.RequestBody.Required)

		reqRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
		reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]
		assert.Contains(t, reqObj.Required, "items")
		require.NotNil(t, reqObj.Properties["items"].Items.MinLength)
		assert.Equal(t, 1, *reqObj.Properties["items"].Items.MinLength)

		var badReq reqDiveReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/req-dive", []byte(`{}`), "application/json", nil), &badReq, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "items", Rule: "required"})
	})
}

func TestCompileOperation_ConditionalCrossFieldAndValidatorPanicPrechecks(t *testing.T) {
	t.Run("rejects_short_circuited_conditional_literals_and_panic_cousins", func(t *testing.T) {
		type nestedItem struct {
			Name string `json:"name"`
		}
		type anyFieldItem struct {
			Dyn any `json:"dyn"`
		}
		type sliceFieldItem struct {
			Tags []string `json:"tags"`
		}
		type doublePtrFieldItem struct {
			Ptr **int `json:"ptr"`
		}

		type reqIfIntOops struct {
			pi.Meta `path:"/req-if-int" method:"POST"`
			Mode    string `json:"mode"`
			Count   int    `json:"count"`
			Target  string `json:"target" validate:"required_if=Mode ready Count oops"`
		}
		type reqUnlessUintBad struct {
			pi.Meta `path:"/req-unless-uint" method:"POST"`
			Mode    string `json:"mode"`
			Port    uint16 `json:"port"`
			Target  string `json:"target" validate:"required_unless=Mode ready Port nope"`
		}
		type exclIfFloat32Bad struct {
			pi.Meta `path:"/excl-if-f32" method:"POST"`
			Mode    string  `json:"mode"`
			Ratio   float32 `json:"ratio"`
			Target  string  `json:"target" validate:"excluded_if=Mode ready Ratio bad"`
		}
		type exclIfFloat64NaN struct {
			pi.Meta `path:"/excl-if-nan" method:"POST"`
			Mode    string  `json:"mode"`
			Score   float64 `json:"score"`
			Target  string  `json:"target" validate:"excluded_if=Mode ready Score NaN"`
		}
		type exclUnlessBoolBad struct {
			pi.Meta `path:"/excl-unless-bool" method:"POST"`
			Mode    string `json:"mode"`
			Flag    bool   `json:"flag"`
			Target  string `json:"target" validate:"excluded_unless=Mode ready Flag yes"`
		}
		type skipUnlessPtrIntNil struct {
			pi.Meta `path:"/skip-unless-ptr-int" method:"POST"`
			Mode    string `json:"mode"`
			Count   *int   `json:"count"`
			Target  string `json:"target" validate:"skip_unless=Mode ready Count nil"`
		}
		type reqIfSliceNonInt struct {
			pi.Meta `path:"/req-if-slice" method:"POST"`
			Mode    string   `json:"mode"`
			Tags    []string `json:"tags"`
			Target  string   `json:"target" validate:"required_if=Mode ready Tags oops"`
		}
		type reqIfArrayNil struct {
			pi.Meta `path:"/req-if-arr-nil" method:"POST"`
			Mode    string   `json:"mode"`
			Fixed   [16]byte `json:"fixed"`
			Target  string   `json:"target" validate:"required_if=Mode ready Fixed nil"`
		}
		type reqIfUUIDNil struct {
			pi.Meta `path:"/req-if-uuid-nil" method:"POST"`
			Mode    string    `json:"mode"`
			ID      uuid.UUID `json:"id"`
			Target  string    `json:"target" validate:"required_if=Mode ready ID nil"`
		}
		type reqIfAnyTarget struct {
			pi.Meta `path:"/req-if-any" method:"POST"`
			Mode    string `json:"mode"`
			Payload any    `json:"payload"`
			Target  string `json:"target" validate:"required_if=Mode ready Payload ok"`
		}
		type orBranchBadCondLit struct {
			pi.Meta `path:"/or-cond-lit" method:"POST"`
			Mode    string `json:"mode"`
			Count   int    `json:"count"`
			Target  string `json:"target" validate:"eq=x|required_if=Mode ready Count oops"`
		}
		type slicePathNoIndex struct {
			pi.Meta `path:"/slice-path" method:"POST"`
			Items   []nestedItem `json:"items"`
			Target  string       `json:"target" validate:"required_with=Items.Name"`
		}
		type arrayPathNoIndex struct {
			pi.Meta `path:"/array-path" method:"POST"`
			Items   [2]nestedItem `json:"items"`
			Target  string        `json:"target" validate:"eqfield=Items.Name"`
		}
		type unexportedFieldPath struct {
			pi.Meta `path:"/unexported-path" method:"POST"`
			secret  string
			Target  string `json:"target" validate:"required_with=secret"`
		}
		type timeSubFieldPath struct {
			pi.Meta `path:"/time-path" method:"POST"`
			Created time.Time `json:"created"`
			Target  string    `json:"target" validate:"required_with=Created.sec"`
		}
		type bracketIndexPath struct {
			pi.Meta `path:"/bracket-path" method:"POST"`
			Items   []nestedItem `json:"items"`
			Target  string       `json:"target" validate:"required_with=Items[0].Name"`
		}
		type postcodeTargetInt struct {
			pi.Meta `path:"/postcode-int" method:"POST"`
			Count   int    `json:"count"`
			Postal  string `json:"postal" validate:"eq=ready,postcode_iso3166_alpha2_field=Count"`
		}
		type postcodeTargetPtrStr struct {
			pi.Meta `path:"/postcode-ptr" method:"POST"`
			Country *string `json:"country"`
			Postal  string  `json:"postal" validate:"postcode_iso3166_alpha2_field=Country"`
		}
		type postcodeFieldInt struct {
			pi.Meta `path:"/postcode-field-int" method:"POST"`
			Country string `json:"country"`
			Postal  int    `json:"postal" validate:"postcode_iso3166_alpha2_field=Country"`
		}
		type reqIfDupField struct {
			pi.Meta `path:"/req-if-dup" method:"POST"`
			Mode    string `json:"mode"`
			Target  string `json:"target" validate:"required_if=Mode a Mode b"`
		}
		type uniqueSliceAny struct {
			pi.Meta `path:"/unique-any" method:"POST"`
			Items   []any `json:"items" validate:"unique"`
		}
		type uniqueArraySlice struct {
			pi.Meta `path:"/unique-arr-slice" method:"POST"`
			Items   [2][]string `json:"items" validate:"unique"`
		}
		type uniqueMapAny struct {
			pi.Meta `path:"/unique-map-any" method:"POST"`
			Attrs   map[string]any `json:"attrs" validate:"unique"`
		}
		type uniqueStructFieldAny struct {
			pi.Meta `path:"/unique-field-any" method:"POST"`
			Items   []anyFieldItem `json:"items" validate:"unique=Dyn"`
		}
		type uniqueStructFieldSlice struct {
			pi.Meta `path:"/unique-field-slice" method:"POST"`
			Items   []sliceFieldItem `json:"items" validate:"unique=Tags"`
		}
		type uniqueStructContainingAny struct {
			pi.Meta `path:"/unique-struct-any" method:"POST"`
			Items   []anyFieldItem `json:"items" validate:"unique"`
		}
		type uniqueSlicePtrElem struct {
			pi.Meta `path:"/unique-slice-ptr" method:"POST"`
			Items   []*string `json:"items" validate:"unique"`
		}
		type uniqueArrayPtrElem struct {
			pi.Meta `path:"/unique-arr-ptr" method:"POST"`
			Items   [2]*string `json:"items" validate:"unique"`
		}
		type uniqueSlicePtrStructField struct {
			pi.Meta `path:"/unique-slice-ptr-field" method:"POST"`
			Items   []*nestedItem `json:"items" validate:"unique=Name"`
		}
		type uniqueMapPtrVal struct {
			pi.Meta `path:"/unique-map-ptr" method:"POST"`
			Attrs   map[string]*string `json:"attrs" validate:"unique"`
		}
		type uniqueDoublePtrField struct {
			pi.Meta `path:"/unique-double-ptr-field" method:"POST"`
			Items   []doublePtrFieldItem `json:"items" validate:"unique=Ptr"`
		}
		type skipUnlessPtrTrailingMin struct {
			pi.Meta `path:"/skip-unless-ptr-min" method:"POST"`
			Mode    string  `json:"mode"`
			Ptr     *string `json:"ptr" validate:"skip_unless=Mode strict,min=3"`
		}
		type requiredIfPtrTrailingMin struct {
			pi.Meta `path:"/req-if-ptr-min" method:"POST"`
			Mode    string  `json:"mode"`
			Ptr     *string `json:"ptr" validate:"required_if=Mode strict,min=3"`
		}
		type requiredIfPtrSliceDive struct {
			pi.Meta `path:"/req-if-ptr-slice-dive" method:"POST"`
			Mode    string    `json:"mode"`
			Items   *[]string `json:"items" validate:"required_if=Mode strict,dive,required"`
		}
		type requiredIfPtrOrBranchMin struct {
			pi.Meta `path:"/req-if-ptr-or-min" method:"POST"`
			Mode    string  `json:"mode"`
			Ptr     *string `json:"ptr" validate:"required_if=Mode strict|min=3"`
		}

		cases := []struct {
			name    string
			reqType reflect.Type
		}{
			{"issue2_required_if_int_oops", reflect.TypeOf((*reqIfIntOops)(nil))},
			{"required_unless_uint_bad", reflect.TypeOf((*reqUnlessUintBad)(nil))},
			{"excluded_if_float32_bad", reflect.TypeOf((*exclIfFloat32Bad)(nil))},
			{"excluded_if_float64_nan", reflect.TypeOf((*exclIfFloat64NaN)(nil))},
			{"excluded_unless_bool_bad", reflect.TypeOf((*exclUnlessBoolBad)(nil))},
			{"skip_unless_ptr_int_nil", reflect.TypeOf((*skipUnlessPtrIntNil)(nil))},
			{"required_if_slice_non_int", reflect.TypeOf((*reqIfSliceNonInt)(nil))},
			{"required_if_array_nil", reflect.TypeOf((*reqIfArrayNil)(nil))},
			{"required_if_uuid_nil", reflect.TypeOf((*reqIfUUIDNil)(nil))},
			{"required_if_any_target", reflect.TypeOf((*reqIfAnyTarget)(nil))},
			{"or_branch_conditional_bad_literal", reflect.TypeOf((*orBranchBadCondLit)(nil))},
			{"cousinA_slice_path_without_index", reflect.TypeOf((*slicePathNoIndex)(nil))},
			{"cousinA_array_path_without_index", reflect.TypeOf((*arrayPathNoIndex)(nil))},
			{"cousinA_unexported_field_path", reflect.TypeOf((*unexportedFieldPath)(nil))},
			{"cousinA_time_sub_field_path", reflect.TypeOf((*timeSubFieldPath)(nil))},
			{"cousinA_bracket_index_path", reflect.TypeOf((*bracketIndexPath)(nil))},
			{"cousinB_postcode_target_int", reflect.TypeOf((*postcodeTargetInt)(nil))},
			{"cousinB_postcode_target_ptr_string", reflect.TypeOf((*postcodeTargetPtrStr)(nil))},
			{"cousinB_postcode_field_int", reflect.TypeOf((*postcodeFieldInt)(nil))},
			{"cousinC_required_if_duplicate_field", reflect.TypeOf((*reqIfDupField)(nil))},
			{"cousinE_unique_slice_any", reflect.TypeOf((*uniqueSliceAny)(nil))},
			{"cousinE_unique_array_slice", reflect.TypeOf((*uniqueArraySlice)(nil))},
			{"cousinE_unique_map_any", reflect.TypeOf((*uniqueMapAny)(nil))},
			{"cousinE_unique_struct_field_any", reflect.TypeOf((*uniqueStructFieldAny)(nil))},
			{"cousinE_unique_struct_field_slice", reflect.TypeOf((*uniqueStructFieldSlice)(nil))},
			{"cousinE_unique_struct_containing_any", reflect.TypeOf((*uniqueStructContainingAny)(nil))},
			{"cousinE_unique_slice_ptr_elem", reflect.TypeOf((*uniqueSlicePtrElem)(nil))},
			{"cousinE_unique_array_ptr_elem", reflect.TypeOf((*uniqueArrayPtrElem)(nil))},
			{"cousinE_unique_slice_ptr_struct_field", reflect.TypeOf((*uniqueSlicePtrStructField)(nil))},
			{"cousinE_unique_map_ptr_val", reflect.TypeOf((*uniqueMapPtrVal)(nil))},
			{"cousinE_unique_double_ptr_field", reflect.TypeOf((*uniqueDoublePtrField)(nil))},
			{"skip_unless_ptr_trailing_min_without_omit", reflect.TypeOf((*skipUnlessPtrTrailingMin)(nil))},
			{"required_if_ptr_trailing_min_without_omit", reflect.TypeOf((*requiredIfPtrTrailingMin)(nil))},
			{"required_if_ptr_slice_dive_without_omit", reflect.TypeOf((*requiredIfPtrSliceDive)(nil))},
			{"required_if_ptr_or_branch_min", reflect.TypeOf((*requiredIfPtrOrBranchMin)(nil))},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := contract.CompileOperation(tc.name, tc.reqType, reflect.TypeOf((*simpleRes)(nil)), contract.CompileOptions{}, nil)
				require.Error(t, err)
			})
		}
	})

	t.Run("accepts_valid_multi_condition_and_nil_literals_and_validates_runtime", func(t *testing.T) {
		type uniqRow struct {
			ID int `json:"id"`
		}
		type validCondReq struct {
			pi.Meta  `path:"/valid-cond" method:"POST"`
			Mode     string    `json:"mode"`
			Count    int       `json:"count"`
			PtrCount *int      `json:"ptrCount"`
			Flag     bool      `json:"flag"`
			Tags     []string  `json:"tags" validate:"unique"`
			Fixed    [2]int    `json:"fixed"`
			Rows     []uniqRow `json:"rows" validate:"unique=ID"`
			Country  string    `json:"country"`
			Postal   string    `json:"postal" validate:"omitempty,postcode_iso3166_alpha2_field=Country"`
			Target   string    `json:"target" validate:"required_if=Mode ready Count 2 PtrCount 5 Flag true Tags 1 Fixed 2"`
			Opt      string    `json:"opt" validate:"required_if=Tags nil"`
		}

		op, _ := compileOp[validCondReq, simpleRes](t, contract.CompileOptions{})

		var shortCircuitReq validCondReq
		require.NoError(t, op.BindAndValidate(
			makePIRequest(http.MethodPost, "/valid-cond", []byte(`{"mode":"other","tags":[]}`), "application/json", nil),
			&shortCircuitReq,
			nil,
		))

		var missingTarget validCondReq
		err := op.BindAndValidate(
			makePIRequest(http.MethodPost, "/valid-cond", []byte(`{"mode":"ready","count":2,"ptrCount":5,"flag":true,"tags":["x"],"fixed":[1,2]}`), "application/json", nil),
			&missingTarget,
			nil,
		)
		require.Error(t, err)
		status, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, status)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "target", Rule: "required_if"})

		var missingOptWhenTagsNil validCondReq
		err = op.BindAndValidate(
			makePIRequest(http.MethodPost, "/valid-cond", []byte(`{"mode":"other"}`), "application/json", nil),
			&missingOptWhenTagsNil,
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "opt", Rule: "required_if"})

		var okReq validCondReq
		require.NoError(t, op.BindAndValidate(
			makePIRequest(http.MethodPost, "/valid-cond", []byte(`{"mode":"ready","count":2,"ptrCount":5,"flag":true,"tags":["x"],"fixed":[1,2],"rows":[{"id":1},{"id":2}],"country":"US","postal":"10001","target":"done"}`), "application/json", nil),
			&okReq,
			nil,
		))
	})
}

func TestSchemaNullability_PointerValidationAndOmitSemantics(t *testing.T) {
	t.Run("slice_of_ptr_string_dive_required_fixes_schema_inversion", func(t *testing.T) {
		type ptrSliceReq struct {
			pi.Meta     `path:"/ptr-slice" method:"POST"`
			Items       []*string `json:"items" validate:"dive,required"`
			OptItems    []*string `json:"optItems" validate:"dive,omitempty,min=3"`
			ValOptItems []string  `json:"valOptItems" validate:"dive,omitempty,min=3"`
		}

		op, sr := compileOp[ptrSliceReq, simpleRes](t, contract.CompileOptions{})
		oasOp := op.BuildOpenAPIOperation(sr)
		reqRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
		reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]

		itemsProp := reqObj.Properties["items"]
		require.NotNil(t, itemsProp.Items)
		assert.Equal(t, "string", itemsProp.Items.Type)
		assert.False(t, itemsProp.Items.Nullable)
		assert.Nil(t, itemsProp.Items.MinLength)

		optItemsProp := reqObj.Properties["optItems"]
		require.NotNil(t, optItemsProp.Items)
		assert.Equal(t, "string", optItemsProp.Items.Type)
		assert.True(t, optItemsProp.Items.Nullable)
		require.NotNil(t, optItemsProp.Items.MinLength)
		assert.Equal(t, 3, *optItemsProp.Items.MinLength)

		valOptProp := reqObj.Properties["valOptItems"]
		require.NotNil(t, valOptProp.Items)
		assert.Equal(t, "string", valOptProp.Items.Type)
		assert.False(t, valOptProp.Items.Nullable)
		assert.Nil(t, valOptProp.Items.MinLength)

		var out ptrSliceReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/ptr-slice", []byte(`{"items":[null]}`), "application/json", nil), &out, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "items[0]", Rule: "required"})

		out = ptrSliceReq{}
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/ptr-slice", []byte(`{"items":[""],"optItems":[null,"abc"],"valOptItems":["","abcd"]}`), "application/json", nil), &out, nil))

		out = ptrSliceReq{}
		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/ptr-slice", []byte(`{"optItems":[""]}`), "application/json", nil), &out, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "optItems[0]", Rule: "min"})

		out = ptrSliceReq{}
		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/ptr-slice", []byte(`{"valOptItems":["ab"]}`), "application/json", nil), &out, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "valOptItems[0]", Rule: "min"})
	})

	t.Run("pointer_fields_and_refs_and_maps_match_runtime_nullability", func(t *testing.T) {
		type pointerSemanticsReq struct {
			pi.Meta        `path:"/ptr-semantics" method:"POST"`
			ReqPtr         *string            `json:"reqPtr" validate:"required"`
			PlainReq       string             `json:"plainReq" validate:"required"`
			MinPtr         *string            `json:"minPtr" validate:"min=3"`
			OmitEmptyStr   string             `json:"omitEmptyStr" validate:"omitempty,min=3"`
			OmitNilStr     string             `json:"omitNilStr" validate:"omitnil,min=3"`
			OmitZeroPtr    *string            `json:"omitZeroPtr" validate:"omitzero,min=3"`
			OmitZeroSlice  []string           `json:"omitZeroSlice" validate:"omitzero,min=2"`
			OmitEmptySlice []string           `json:"omitEmptySlice" validate:"omitempty,min=2"`
			Child          *nullableChild     `json:"child" validate:"required"`
			ChildDesc      *nullableChild     `json:"childDesc" desc:"Required child" validate:"required"`
			Children       []*nullableChild   `json:"children" validate:"dive,required"`
			Labels         map[string]*string `json:"labels" validate:"dive,required"`
			KeyValMap      map[string]string  `json:"keyValMap" validate:"dive,keys,min=2,endkeys,required"`
			KeyPtrMap      map[string]*string `json:"keyPtrMap" validate:"dive,keys,min=2,endkeys,omitempty,min=3"`
			CondPtr        *string            `json:"condPtr" validate:"required_if=PlainReq need"`
			QueryPtr       *string            `query:"qptr" validate:"omitempty,min=3"`
			QueryVal       string             `query:"qval" validate:"omitempty,min=3"`
		}

		op, sr := compileOp[pointerSemanticsReq, simpleRes](t, contract.CompileOptions{})
		oasOp := op.BuildOpenAPIOperation(sr)
		reqRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
		reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]

		assert.Contains(t, reqObj.Required, "reqPtr")
		assert.Contains(t, reqObj.Required, "plainReq")
		assert.Contains(t, reqObj.Required, "child")
		assert.Contains(t, reqObj.Required, "childDesc")

		assert.False(t, reqObj.Properties["reqPtr"].Nullable)
		assert.Nil(t, reqObj.Properties["reqPtr"].MinLength)
		assert.False(t, reqObj.Properties["plainReq"].Nullable)
		assert.Nil(t, reqObj.Properties["plainReq"].MinLength)

		assert.False(t, reqObj.Properties["minPtr"].Nullable)
		require.NotNil(t, reqObj.Properties["minPtr"].MinLength)
		assert.Equal(t, 3, *reqObj.Properties["minPtr"].MinLength)

		assert.Nil(t, reqObj.Properties["omitEmptyStr"].MinLength)
		require.NotNil(t, reqObj.Properties["omitNilStr"].MinLength)
		assert.Equal(t, 3, *reqObj.Properties["omitNilStr"].MinLength)

		assert.True(t, reqObj.Properties["omitZeroPtr"].Nullable)
		assert.Nil(t, reqObj.Properties["omitZeroPtr"].MinLength)

		assert.Nil(t, reqObj.Properties["omitZeroSlice"].MinItems)
		require.NotNil(t, reqObj.Properties["omitEmptySlice"].MinItems)
		assert.Equal(t, 2, *reqObj.Properties["omitEmptySlice"].MinItems)

		childProp := reqObj.Properties["child"]
		assert.Equal(t, "#/components/schemas/nullableChild", childProp.Ref)
		assert.Empty(t, childProp.AllOf)
		assert.False(t, childProp.Nullable)

		childDescProp := reqObj.Properties["childDesc"]
		assert.Empty(t, childDescProp.Ref)
		assert.Equal(t, "Required child", childDescProp.Description)
		assert.False(t, childDescProp.Nullable)
		require.Len(t, childDescProp.AllOf, 1)
		assert.Equal(t, "#/components/schemas/nullableChild", childDescProp.AllOf[0].Ref)

		childrenItems := reqObj.Properties["children"].Items
		require.NotNil(t, childrenItems)
		assert.Equal(t, "#/components/schemas/nullableChild", childrenItems.Ref)
		assert.Empty(t, childrenItems.AllOf)
		assert.False(t, childrenItems.Nullable)

		labelsVal := reqObj.Properties["labels"].AdditionalProperties.Schema
		require.NotNil(t, labelsVal)
		assert.False(t, labelsVal.Nullable)
		assert.Nil(t, labelsVal.MinLength)

		keyVal := reqObj.Properties["keyValMap"].AdditionalProperties.Schema
		require.NotNil(t, keyVal)
		assert.False(t, keyVal.Nullable)
		require.NotNil(t, keyVal.MinLength)
		assert.Equal(t, 1, *keyVal.MinLength)

		keyPtrVal := reqObj.Properties["keyPtrMap"].AdditionalProperties.Schema
		require.NotNil(t, keyPtrVal)
		assert.True(t, keyPtrVal.Nullable)
		require.NotNil(t, keyPtrVal.MinLength)
		assert.Equal(t, 3, *keyPtrVal.MinLength)

		assert.True(t, reqObj.Properties["condPtr"].Nullable)

		paramByName := make(map[string]*openapi.Parameter, len(oasOp.Parameters))
		for _, p := range oasOp.Parameters {
			paramByName[p.Name] = p
		}
		require.NotNil(t, paramByName["qptr"].Schema.MinLength)
		assert.Equal(t, 3, *paramByName["qptr"].Schema.MinLength)
		assert.Nil(t, paramByName["qval"].Schema.MinLength)

		validBody := `{"reqPtr":"","plainReq":"ok","minPtr":"abc","omitEmptyStr":"","omitNilStr":"abc","omitZeroPtr":"","omitZeroSlice":[],"omitEmptySlice":["a","b"],"child":{"code":"c1"},"childDesc":{"code":"c2"},"children":[{"code":"c3"}],"labels":{"k":""},"keyValMap":{"ab":"v"},"keyPtrMap":{"ab":null},"condPtr":null}`
		var out pointerSemanticsReq
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/ptr-semantics?qval=", []byte(validBody), "application/json", nil), &out, nil))

		failCases := []struct {
			name  string
			url   string
			body  string
			field string
			rule  string
		}{
			{
				name:  "reqPtr_null_rejected",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":null,"plainReq":"ok","minPtr":"abc","omitNilStr":"abc","child":{"code":"c1"},"childDesc":{"code":"c2"}}`,
				field: "reqPtr",
				rule:  "required",
			},
			{
				name:  "omitNilStr_empty_rejected",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":"","plainReq":"ok","minPtr":"abc","omitNilStr":"","child":{"code":"c1"},"childDesc":{"code":"c2"}}`,
				field: "omitNilStr",
				rule:  "min",
			},
			{
				name:  "omitEmptySlice_empty_rejected",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":"","plainReq":"ok","minPtr":"abc","omitNilStr":"abc","omitEmptySlice":[],"child":{"code":"c1"},"childDesc":{"code":"c2"}}`,
				field: "omitEmptySlice",
				rule:  "min",
			},
			{
				name:  "children_null_elem_rejected",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":"","plainReq":"ok","minPtr":"abc","omitNilStr":"abc","child":{"code":"c1"},"childDesc":{"code":"c2"},"children":[null]}`,
				field: "children[0]",
				rule:  "required",
			},
			{
				name:  "labels_null_val_rejected",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":"","plainReq":"ok","minPtr":"abc","omitNilStr":"abc","child":{"code":"c1"},"childDesc":{"code":"c2"},"labels":{"k":null}}`,
				field: "labels[k]",
				rule:  "required",
			},
			{
				name:  "keyValMap_empty_val_rejected",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":"","plainReq":"ok","minPtr":"abc","omitNilStr":"abc","child":{"code":"c1"},"childDesc":{"code":"c2"},"keyValMap":{"ab":""}}`,
				field: "keyValMap[ab]",
				rule:  "required",
			},
			{
				name:  "condPtr_null_rejected_when_triggered",
				url:   "/ptr-semantics",
				body:  `{"reqPtr":"","plainReq":"need","minPtr":"abc","omitNilStr":"abc","child":{"code":"c1"},"childDesc":{"code":"c2"},"condPtr":null}`,
				field: "condPtr",
				rule:  "required_if",
			},
			{
				name:  "queryPtr_short_rejected",
				url:   "/ptr-semantics?qptr=ab",
				body:  validBody,
				field: "qptr",
				rule:  "min",
			},
		}

		for _, tc := range failCases {
			t.Run(tc.name, func(t *testing.T) {
				var target pointerSemanticsReq
				err := op.BindAndValidate(makePIRequest(http.MethodPost, tc.url, []byte(tc.body), "application/json", nil), &target, nil)
				require.Error(t, err)
				assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: tc.field, Rule: tc.rule})
			})
		}
	})

	t.Run("structonly_and_conditional_pointer_omit_parity", func(t *testing.T) {
		type structOnlyAndCondReq struct {
			pi.Meta           `path:"/structonly-cond" method:"POST"`
			Mode              string         `json:"mode"`
			StructOnlyPtr     *nullableChild `json:"structOnlyPtr" validate:"structonly"`
			OmitStructOnlyPtr *nullableChild `json:"omitStructOnlyPtr" validate:"omitempty,structonly"`
			CondOmitMinPtr    *string        `json:"condOmitMinPtr" validate:"required_if=Mode strict,omitempty,min=3"`
		}

		op, sr := compileOp[structOnlyAndCondReq, simpleRes](t, contract.CompileOptions{})
		oasOp := op.BuildOpenAPIOperation(sr)
		reqRef := oasOp.RequestBody.Content["application/json"].Schema.Ref
		reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]

		structOnlyProp := reqObj.Properties["structOnlyPtr"]
		assert.Equal(t, "#/components/schemas/nullableChild", structOnlyProp.Ref)
		assert.Empty(t, structOnlyProp.AllOf)
		assert.False(t, structOnlyProp.Nullable)

		omitStructOnlyProp := reqObj.Properties["omitStructOnlyPtr"]
		assert.Empty(t, omitStructOnlyProp.Ref)
		assert.True(t, omitStructOnlyProp.Nullable)
		require.Len(t, omitStructOnlyProp.AllOf, 1)
		assert.Equal(t, "#/components/schemas/nullableChild", omitStructOnlyProp.AllOf[0].Ref)

		condMinProp := reqObj.Properties["condOmitMinPtr"]
		assert.Equal(t, "string", condMinProp.Type)
		assert.True(t, condMinProp.Nullable)
		require.NotNil(t, condMinProp.MinLength)
		assert.Equal(t, 3, *condMinProp.MinLength)

		var okRelaxed structOnlyAndCondReq
		require.NoError(t, op.BindAndValidate(
			makePIRequest(http.MethodPost, "/structonly-cond", []byte(`{"mode":"relaxed","structOnlyPtr":{"code":"c1"},"omitStructOnlyPtr":null,"condOmitMinPtr":null}`), "application/json", nil),
			&okRelaxed,
			nil,
		))

		var okStrict structOnlyAndCondReq
		require.NoError(t, op.BindAndValidate(
			makePIRequest(http.MethodPost, "/structonly-cond", []byte(`{"mode":"strict","structOnlyPtr":{"code":"c1"},"omitStructOnlyPtr":{"code":"c2"},"condOmitMinPtr":"abc"}`), "application/json", nil),
			&okStrict,
			nil,
		))

		var badStructOnlyNil structOnlyAndCondReq
		err := op.BindAndValidate(
			makePIRequest(http.MethodPost, "/structonly-cond", []byte(`{"mode":"relaxed","structOnlyPtr":null,"omitStructOnlyPtr":null,"condOmitMinPtr":null}`), "application/json", nil),
			&badStructOnlyNil,
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "structOnlyPtr", Rule: "structonly"})

		var badCondStrictNil structOnlyAndCondReq
		err = op.BindAndValidate(
			makePIRequest(http.MethodPost, "/structonly-cond", []byte(`{"mode":"strict","structOnlyPtr":{"code":"c1"},"condOmitMinPtr":null}`), "application/json", nil),
			&badCondStrictNil,
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "condOmitMinPtr", Rule: "required_if"})

		var badCondMin structOnlyAndCondReq
		err = op.BindAndValidate(
			makePIRequest(http.MethodPost, "/structonly-cond", []byte(`{"mode":"relaxed","structOnlyPtr":{"code":"c1"},"condOmitMinPtr":"ab"}`), "application/json", nil),
			&badCondMin,
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "condOmitMinPtr", Rule: "min"})
	})
}

func makeMultipartRequest(t *testing.T, method, target string, fields map[string][]string, files map[string][]struct {
	filename string
	content  []byte
}) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for k, vals := range fields {
		for _, v := range vals {
			require.NoError(t, mw.WriteField(k, v))
		}
	}
	for fieldName, fileList := range files {
		for _, f := range fileList {
			part, err := mw.CreateFormFile(fieldName, f.filename)
			require.NoError(t, err)
			_, err = part.Write(f.content)
			require.NoError(t, err)
		}
	}
	require.NoError(t, mw.Close())

	req := httptest.NewRequest(method, target, bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func TestUnifiedRouting_OptionalMetaAndMaxBody(t *testing.T) {
	t.Run("no_meta_all_7_methods_and_bodyless_rules", func(t *testing.T) {
		type queryOnlyReq struct {
			Page int `query:"page" default:"1" validate:"min=1"`
		}
		type jsonReq struct {
			Name string `json:"name" validate:"required"`
		}

		for _, method := range []string{http.MethodGet, http.MethodDelete, http.MethodHead, http.MethodOptions} {
			op, sr := compileOp[queryOnlyReq, simpleRes](t, contract.CompileOptions{
				GroupPrefix:      "/api/v1",
				HasExplicitRoute: true,
				ExplicitMethod:   method,
				ExplicitPattern:  "items",
				InheritedTags:    []string{"Items"},
			})
			assert.Equal(t, method, op.Method)
			assert.Equal(t, "/items", op.ChiPattern)
			assert.Equal(t, "/api/v1/items", op.CanonicalPath)
			assert.Equal(t, 200, op.Status)
			assert.Equal(t, []string{"Items"}, op.Tags)
			assert.Equal(t, int64(1<<20), op.MaxBodyBytes)

			var out queryOnlyReq
			require.NoError(t, op.BindAndValidate(makePIRequest(method, "/api/v1/items", nil, "", nil), &out, nil))
			assert.Equal(t, 1, out.Page)

			// Bodyless methods (GET, DELETE, HEAD, OPTIONS) must reject even "{}" body.
			err := op.BindAndValidate(makePIRequest(method, "/api/v1/items", []byte("{}"), "application/json", nil), &out, nil)
			require.Error(t, err)
			errStatus, _, _ := piHTTP.ErrorResponse(err)
			assert.Equal(t, http.StatusBadRequest, errStatus)

			oasOp := op.BuildOpenAPIOperation(sr)
			assert.Nil(t, oasOp.RequestBody)
			assert.Contains(t, oasOp.Responses, "200")

			// Bodyless methods reject JSON body fields at compile time.
			_, compileErr := contract.CompileOperation("h", reflect.TypeOf((*jsonReq)(nil)), reflect.TypeOf((*simpleRes)(nil)), contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   method,
				ExplicitPattern:  "/items",
			}, contract.NewSchemaRegistry())
			assert.Error(t, compileErr)
		}

		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch} {
			op, _ := compileOp[jsonReq, simpleRes](t, contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   method,
				ExplicitPattern:  "/items",
			})
			assert.Equal(t, method, op.Method)
			assert.Equal(t, 200, op.Status)

			var out jsonReq
			require.NoError(t, op.BindAndValidate(makePIRequest(method, "/items", []byte(`{"name":"ok"}`), "application/json", nil), &out, nil))
			assert.Equal(t, "ok", out.Name)
		}
	})

	t.Run("explicit_pattern_normalization_and_meta_matching", func(t *testing.T) {
		type plainReq struct{}
		opRoot, _ := compileOp[plainReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "",
		})
		assert.Equal(t, "/", opRoot.ChiPattern)
		assert.Equal(t, "/", opRoot.CanonicalPath)
		assert.Equal(t, "getRoot", opRoot.OperationID)

		// Trailing slash on non-root explicit pattern must fail.
		_, err := contract.CompileOperation("h", reflect.TypeOf((*plainReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
			GroupPrefix:      "/api/v1",
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/posts/",
		}, contract.NewSchemaRegistry())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "/posts")

		// Meta without path/method inherits explicit route.
		type metaNoRouteReq struct {
			pi.Meta `status:"201" summary:"Create post"`
			Title   string `json:"title"`
		}
		opNoRoute, _ := compileOp[metaNoRouteReq, simpleRes](t, contract.CompileOptions{
			GroupPrefix:      "/api/v1",
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/posts",
		})
		assert.Equal(t, "/api/v1/posts", opNoRoute.CanonicalPath)
		assert.Equal(t, 201, opNoRoute.Status)
		assert.Equal(t, "Create post", opNoRoute.Summary)

		// Matching method/path succeeds.
		type metaMatchReq struct {
			pi.Meta `path:"/posts" method:"POST"`
		}
		opMatch, _ := compileOp[metaMatchReq, emptyRes](t, contract.CompileOptions{
			GroupPrefix:      "/api/v1",
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "posts",
		})
		assert.Equal(t, "/api/v1/posts", opMatch.CanonicalPath)

		// Root "" + Meta path:"" matches.
		type metaEmptyPathReq struct {
			pi.Meta `path:""`
		}
		opRootEmptyMeta, _ := compileOp[metaEmptyPathReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "",
		})
		assert.Equal(t, "/", opRootEmptyMeta.CanonicalPath)

		// Root /posts + Meta path:"" reports path mismatch (not "root group requires path starting with '/'").
		_, err = contract.CompileOperation("h", reflect.TypeOf((*metaEmptyPathReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/posts",
		}, contract.NewSchemaRegistry())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path mismatch")

		// Mismatched method fails.
		type metaWrongMethodReq struct {
			pi.Meta `method:"PUT"`
		}
		_, err = contract.CompileOperation("h", reflect.TypeOf((*metaWrongMethodReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/posts",
		}, contract.NewSchemaRegistry())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "method mismatch")

		// Empty method:"" fails.
		type metaEmptyMethodReq struct {
			pi.Meta `method:""`
		}
		_, err = contract.CompileOperation("h", reflect.TypeOf((*metaEmptyMethodReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/posts",
		}, contract.NewSchemaRegistry())
		require.Error(t, err)

		// Mismatched path fails.
		type metaWrongPathReq struct {
			pi.Meta `path:"/other"`
		}
		_, err = contract.CompileOperation("h", reflect.TypeOf((*metaWrongPathReq)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
			GroupPrefix:      "/api/v1",
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/posts",
		}, contract.NewSchemaRegistry())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path mismatch")
	})

	t.Run("maxBody_tag_precedence_and_validation", func(t *testing.T) {
		type metaMaxBodyReq struct {
			pi.Meta `maxBody:"6MiB"`
			Title   string `form:"title"`
		}
		type fileUploadReq struct {
			File *multipart.FileHeader `form:"file"`
		}
		type fileUploadWithMetaReq struct {
			pi.Meta `maxBody:"5242880"`
			File    *multipart.FileHeader `form:"file"`
		}

		// 1. EndpointMaxBodyBytes > maxBody > ExplicitAppMaxBodyBytes > 32MiB(file)/1MiB(default)
		op1, _ := compileOp[metaMaxBodyReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute:        true,
			ExplicitMethod:          http.MethodPost,
			ExplicitPattern:         "/upload",
			EndpointMaxBodyBytes:    10 << 20,
			ExplicitAppMaxBodyBytes: 2 << 20,
		})
		assert.Equal(t, int64(10<<20), op1.MaxBodyBytes)

		// 2. maxBody > ExplicitAppMaxBodyBytes
		op2, _ := compileOp[metaMaxBodyReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute:        true,
			ExplicitMethod:          http.MethodPost,
			ExplicitPattern:         "/upload",
			ExplicitAppMaxBodyBytes: 2 << 20,
		})
		assert.Equal(t, int64(6<<20), op2.MaxBodyBytes)

		// 3. ExplicitAppMaxBodyBytes overrides 32 MiB file default (even if set to 1 MiB)
		op3, _ := compileOp[fileUploadReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute:        true,
			ExplicitMethod:          http.MethodPost,
			ExplicitPattern:         "/upload",
			ExplicitAppMaxBodyBytes: 1 << 20,
		})
		assert.Equal(t, int64(1<<20), op3.MaxBodyBytes)

		// 4. File default is 32 MiB
		op4, _ := compileOp[fileUploadReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/upload",
		})
		assert.Equal(t, int64(32<<20), op4.MaxBodyBytes)
		assert.True(t, op4.HasFileFields)

		// 5. Numeric byte string in maxBody
		op5, _ := compileOp[fileUploadWithMetaReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/upload",
		})
		assert.Equal(t, int64(5242880), op5.MaxBodyBytes)

		// Invalid maxBody tags
		type zeroMaxBody struct {
			pi.Meta `path:"/x" method:"POST" maxBody:"0"`
		}
		type negMaxBody struct {
			pi.Meta `path:"/x" method:"POST" maxBody:"-5MB"`
		}
		type overMaxBody struct {
			pi.Meta `path:"/x" method:"POST" maxBody:"2GB"`
		}
		type overflowMaxBody struct {
			pi.Meta `path:"/x" method:"POST" maxBody:"999999999999GB"`
		}
		type badUnitMaxBody struct {
			pi.Meta `path:"/x" method:"POST" maxBody:"10TB"`
		}
		for _, rt := range []reflect.Type{
			reflect.TypeOf((*zeroMaxBody)(nil)),
			reflect.TypeOf((*negMaxBody)(nil)),
			reflect.TypeOf((*overMaxBody)(nil)),
			reflect.TypeOf((*overflowMaxBody)(nil)),
			reflect.TypeOf((*badUnitMaxBody)(nil)),
		} {
			_, err := contract.CompileOperation("h", rt, reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{}, contract.NewSchemaRegistry())
			assert.Error(t, err)
		}

		// BindAndValidate with piHTTP.NewRequest (no snapshot) respects op.MaxBodyBytes (> 1 MiB).
		bigContent := bytes.Repeat([]byte("x"), 2<<20) // 2 MiB (> default 1 MiB, < 32 MiB)
		bigReq := makeMultipartRequest(t, http.MethodPost, "/upload", nil, map[string][]struct {
			filename string
			content  []byte
		}{
			"file": {{filename: "big.bin", content: bigContent}},
		})
		var target fileUploadReq
		require.NoError(t, op4.BindAndValidate(piHTTP.NewRequest(bigReq), &target, nil))
		require.NotNil(t, target.File)
		assert.Equal(t, "big.bin", target.File.Filename)
		if bigReq.MultipartForm != nil {
			_ = bigReq.MultipartForm.RemoveAll()
		}
	})
}

func TestMultipartFormAndFileContract(t *testing.T) {
	t.Run("compile_time_tag_and_validation_guards", func(t *testing.T) {
		type legacyFileTagTop struct {
			File *multipart.FileHeader `file:"avatar"`
		}
		type nestedFormTag struct {
			Inner struct {
				Name string `form:"name"`
			} `json:"inner"`
		}
		type nestedFileTag struct {
			Inner struct {
				Name string `file:"name"`
			} `json:"inner"`
		}
		type resFormTag struct {
			Name string `form:"name"`
		}
		type resFileTag struct {
			Name string `file:"name"`
		}
		type untaggedFileHeaderInJSON struct {
			File *multipart.FileHeader `json:"file"`
		}
		type untaggedFileHeaderInRes struct {
			File *multipart.FileHeader `json:"file"`
		}
		type valueFileHeader struct {
			File multipart.FileHeader `form:"file"`
		}
		type valueFileSlice struct {
			Files []multipart.FileHeader `form:"files"`
		}
		type formWithJSONOption struct {
			Title string `form:"title" json:",omitempty"`
		}
		type formWithComma struct {
			Title string `form:"title,omitempty"`
		}
		type dupFormName struct {
			A string `form:"name"`
			B string `form:"name"`
		}
		type mixedFormAndJSON struct {
			A string `form:"a"`
			B string `json:"b"`
		}
		type fileWithDefault struct {
			File *multipart.FileHeader `form:"file" default:"x"`
		}
		type fileWithExample struct {
			File *multipart.FileHeader `form:"file" example:"x"`
		}
		type fileBadRuleEndswith struct {
			File *multipart.FileHeader `form:"file" validate:"endswith=.png"`
		}
		type fileBadRuleEqfield struct {
			Title string                `form:"title"`
			File  *multipart.FileHeader `form:"file" validate:"eqfield=Title"`
		}
		type siblingRefFileEqfield struct {
			Title string                `form:"title" validate:"eqfield=File"`
			File  *multipart.FileHeader `form:"file"`
		}
		type siblingRefFileReqIfBadLit struct {
			Title string                `form:"title" validate:"required_if=File foo"`
			File  *multipart.FileHeader `form:"file"`
		}
		type siblingRefFileFilename struct {
			Title string                `form:"title" validate:"required_with=File.Filename"`
			File  *multipart.FileHeader `form:"file"`
		}
		type fileSliceBadRule struct {
			Files []*multipart.FileHeader `form:"files" validate:"dive,email"`
		}

		badReqs := []reflect.Type{
			reflect.TypeOf((*legacyFileTagTop)(nil)),
			reflect.TypeOf((*nestedFormTag)(nil)),
			reflect.TypeOf((*nestedFileTag)(nil)),
			reflect.TypeOf((*untaggedFileHeaderInJSON)(nil)),
			reflect.TypeOf((*valueFileHeader)(nil)),
			reflect.TypeOf((*valueFileSlice)(nil)),
			reflect.TypeOf((*formWithJSONOption)(nil)),
			reflect.TypeOf((*formWithComma)(nil)),
			reflect.TypeOf((*dupFormName)(nil)),
			reflect.TypeOf((*mixedFormAndJSON)(nil)),
			reflect.TypeOf((*fileWithDefault)(nil)),
			reflect.TypeOf((*fileWithExample)(nil)),
			reflect.TypeOf((*fileBadRuleEndswith)(nil)),
			reflect.TypeOf((*fileBadRuleEqfield)(nil)),
			reflect.TypeOf((*siblingRefFileEqfield)(nil)),
			reflect.TypeOf((*siblingRefFileReqIfBadLit)(nil)),
			reflect.TypeOf((*siblingRefFileFilename)(nil)),
			reflect.TypeOf((*fileSliceBadRule)(nil)),
		}
		for _, rt := range badReqs {
			_, err := contract.CompileOperation("h", rt, reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   http.MethodPost,
				ExplicitPattern:  "/upload",
			}, contract.NewSchemaRegistry())
			assert.Error(t, err, "expected compile error for %v", rt)
		}

		// Forbidden form/file tags and FileHeader in Response structs.
		type okReq struct{}
		for _, resT := range []reflect.Type{
			reflect.TypeOf((*resFormTag)(nil)),
			reflect.TypeOf((*resFileTag)(nil)),
			reflect.TypeOf((*untaggedFileHeaderInRes)(nil)),
		} {
			_, err := contract.CompileOperation("h", reflect.TypeOf((*okReq)(nil)), resT, contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   http.MethodGet,
				ExplicitPattern:  "/x",
			}, contract.NewSchemaRegistry())
			assert.Error(t, err, "expected compile error for res %v", resT)
		}
	})

	t.Run("multipart_binding_validation_security_and_openapi", func(t *testing.T) {
		type uploadReq struct {
			pi.Meta `strict:"true" summary:"Upload media"`
			Mode    string                  `form:"mode" default:"single"`
			Tags    []string                `form:"tags"`
			OptInt  *int                    `form:"optInt"`
			File    *multipart.FileHeader   `form:"file" json:"-" validate:"required_if=Mode single"`
			Gallery []*multipart.FileHeader `form:"gallery" validate:"omitempty,min=1,max=3,dive,required"`
			Check   string                  `form:"check" validate:"required_if=File nil"`
		}

		op, sr := compileOp[uploadReq, simpleRes](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/media",
		})
		assert.True(t, op.HasFormFields)
		assert.True(t, op.HasFileFields)
		assert.False(t, op.HasBodyFields)

		// OpenAPI checks: multipart/form-data only, non-nullable pointers, no minLength on binary items, additionalProperties: false.
		oasOp := op.BuildOpenAPIOperation(sr)
		require.NotNil(t, oasOp.RequestBody)
		assert.Contains(t, oasOp.RequestBody.Content, "multipart/form-data")
		assert.NotContains(t, oasOp.RequestBody.Content, "application/x-www-form-urlencoded")
		assert.Contains(t, oasOp.Responses, "415")

		reqRef := oasOp.RequestBody.Content["multipart/form-data"].Schema.Ref
		require.NotEmpty(t, reqRef)
		reqObj := sr.Schemas()[strings.TrimPrefix(reqRef, "#/components/schemas/")]
		require.NotNil(t, reqObj)
		require.NotNil(t, reqObj.AdditionalProperties)
		require.NotNil(t, reqObj.AdditionalProperties.Allowed)
		assert.False(t, *reqObj.AdditionalProperties.Allowed)

		assert.Equal(t, "string", reqObj.Properties["file"].Type)
		assert.Equal(t, "binary", reqObj.Properties["file"].Format)
		assert.False(t, reqObj.Properties["file"].Nullable)
		assert.False(t, reqObj.Properties["optInt"].Nullable)
		assert.Equal(t, "array", reqObj.Properties["gallery"].Type)
		require.NotNil(t, reqObj.Properties["gallery"].MinItems)
		assert.Equal(t, 1, *reqObj.Properties["gallery"].MinItems)
		assert.Nil(t, reqObj.Properties["gallery"].Items.MinLength)

		// Valid multipart upload with repeated slice keys (no comma splitting).
		req := makeMultipartRequest(t, http.MethodPost, "/media",
			map[string][]string{
				"tags": {"a,b", "c"},
			},
			map[string][]struct {
				filename string
				content  []byte
			}{
				"file": {{filename: "photo.png", content: []byte("PNGDATA")}},
			},
		)
		var out uploadReq
		require.NoError(t, op.BindAndValidate(piHTTP.NewRequest(req), &out, nil))
		assert.Equal(t, "single", out.Mode)
		assert.Equal(t, []string{"a,b", "c"}, out.Tags)
		require.NotNil(t, out.File)
		assert.Equal(t, "photo.png", out.File.Filename)
		// RawReq.MultipartForm must remain populated (cleanup happens in handler.ServeHTTP).
		assert.NotNil(t, req.MultipartForm)
		_ = req.MultipartForm.RemoveAll()

		// Empty <input type="file"> (filename="" in form.Value) allowed in strict mode when optional/triggered via Check.
		reqEmptyInput := makeMultipartRequest(t, http.MethodPost, "/media",
			map[string][]string{
				"mode":  {"optional"},
				"file":  {""},
				"check": {"ok"},
			},
			nil,
		)
		var outEmpty uploadReq
		require.NoError(t, op.BindAndValidate(piHTTP.NewRequest(reqEmptyInput), &outEmpty, nil))
		assert.Nil(t, outEmpty.File)
		_ = reqEmptyInput.MultipartForm.RemoveAll()

		// Path traversal filenames rejected with 400 format.
		for _, badName := range []string{"..", ".", `dir\evil.png`} {
			var mpBuf bytes.Buffer
			mw := multipart.NewWriter(&mpBuf)
			require.NoError(t, mw.WriteField("mode", "single"))
			part, err := mw.CreateFormFile("file", badName)
			require.NoError(t, err)
			_, err = part.Write([]byte("x"))
			require.NoError(t, err)
			require.NoError(t, mw.Close())

			rawBody := mpBuf.Bytes()
			badReq := httptest.NewRequest(http.MethodPost, "/media", bytes.NewReader(rawBody))
			badReq.Header.Set("Content-Type", mw.FormDataContentType())
			var target uploadReq
			err = op.BindAndValidate(piHTTP.NewRequestWithSnapshot(badReq, nil, rawBody, true), &target, nil)
			require.Error(t, err, "expected error for filename %q", badName)
			assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "file", Rule: "format"})
			if badReq.MultipartForm != nil {
				_ = badReq.MultipartForm.RemoveAll()
			}
		}

		// Multiple files on single-file field -> single_value.
		reqMultiOnSingle := makeMultipartRequest(t, http.MethodPost, "/media", nil,
			map[string][]struct {
				filename string
				content  []byte
			}{
				"file": {
					{filename: "a.png", content: []byte("1")},
					{filename: "b.png", content: []byte("2")},
				},
			},
		)
		var outMulti uploadReq
		err := op.BindAndValidate(piHTTP.NewRequest(reqMultiOnSingle), &outMulti, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "file", Rule: "single_value"})
		_ = reqMultiOnSingle.MultipartForm.RemoveAll()

		// File uploaded to scalar field -> type.
		reqFileToScalar := makeMultipartRequest(t, http.MethodPost, "/media", nil,
			map[string][]struct {
				filename string
				content  []byte
			}{
				"mode": {{filename: "a.txt", content: []byte("1")}},
				"file": {{filename: "b.png", content: []byte("2")}},
			},
		)
		var outScalarFile uploadReq
		err = op.BindAndValidate(piHTTP.NewRequest(reqFileToScalar), &outScalarFile, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "mode", Rule: "type"})
		_ = reqFileToScalar.MultipartForm.RemoveAll()

		// Non-empty text in file field -> type.
		reqTextToFile := makeMultipartRequest(t, http.MethodPost, "/media",
			map[string][]string{"file": {"not-a-file"}},
			nil,
		)
		var outTextFile uploadReq
		err = op.BindAndValidate(piHTTP.NewRequest(reqTextToFile), &outTextFile, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "file", Rule: "type"})
		_ = reqTextToFile.MultipartForm.RemoveAll()

		// Unknown form field in strict mode -> unknown_field.
		reqUnknown := makeMultipartRequest(t, http.MethodPost, "/media",
			map[string][]string{"extra": {"1"}},
			map[string][]struct {
				filename string
				content  []byte
			}{
				"file": {{filename: "a.png", content: []byte("1")}},
			},
		)
		var outUnknown uploadReq
		err = op.BindAndValidate(piHTTP.NewRequest(reqUnknown), &outUnknown, nil)
		require.Error(t, err)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "extra", Rule: "unknown_field"})
		_ = reqUnknown.MultipartForm.RemoveAll()

		// Empty boundary -> 415; empty body with multipart/form-data -> 400; x-www-form-urlencoded with file field -> 415.
		var target uploadReq
		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/media", []byte("abc"), "multipart/form-data", nil), &target, nil)
		st415, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusUnsupportedMediaType, st415)

		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/media", []byte{}, "multipart/form-data; boundary=abc", nil), &target, nil)
		st400, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, st400)

		err = op.BindAndValidate(makePIRequest(http.MethodPost, "/media", []byte("mode=x"), "application/x-www-form-urlencoded", nil), &target, nil)
		stURL415, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusUnsupportedMediaType, stURL415)
	})

	t.Run("scalar_form_dual_media_types_and_schema_isolation", func(t *testing.T) {
		type scalarFormReq struct {
			Title string `form:"title" validate:"required"`
			Count int    `form:"count" default:"5"`
		}
		op, sr := compileOp[scalarFormReq, emptyRes](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodPost,
			ExplicitPattern:  "/submit",
		})
		assert.True(t, op.HasFormFields)
		assert.False(t, op.HasFileFields)

		oasOp := op.BuildOpenAPIOperation(sr)
		require.NotNil(t, oasOp.RequestBody)
		mtMP := oasOp.RequestBody.Content["multipart/form-data"]
		mtURL := oasOp.RequestBody.Content["application/x-www-form-urlencoded"]
		require.NotNil(t, mtMP)
		require.NotNil(t, mtURL)
		assert.NotSame(t, mtMP.Schema, mtURL.Schema)

		// urlencoded binding
		var out scalarFormReq
		require.NoError(t, op.BindAndValidate(makePIRequest(http.MethodPost, "/submit", []byte("title=hello"), "application/x-www-form-urlencoded", nil), &out, nil))
		assert.Equal(t, "hello", out.Title)
		assert.Equal(t, 5, out.Count)

		// empty body without Content-Type triggers required validation (400)
		var outEmpty scalarFormReq
		err := op.BindAndValidate(makePIRequest(http.MethodPost, "/submit", nil, "", nil), &outEmpty, nil)
		require.Error(t, err)
		errStatus, _, _ := piHTTP.ErrorResponse(err)
		assert.Equal(t, http.StatusBadRequest, errStatus)
		assert.Contains(t, piHTTP.ErrorDetails(err), apperror.Detail{Field: "title", Rule: "required"})

		// Local same-named structs across two functions/modes must not collide or overwrite in SchemaRegistry.
		srIsolated := contract.NewSchemaRegistry()
		registerLocal1 := func() *contract.CompiledOperation {
			type item struct {
				A string `json:"a"`
			}
			opA, err := contract.CompileOperation("a", reflect.TypeOf((*item)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   http.MethodPost,
				ExplicitPattern:  "/a",
			}, srIsolated)
			require.NoError(t, err)
			return opA
		}
		registerLocal2 := func() *contract.CompiledOperation {
			type item struct {
				B int `form:"b"`
			}
			opB, err := contract.CompileOperation("b", reflect.TypeOf((*item)(nil)), reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   http.MethodPost,
				ExplicitPattern:  "/b",
			}, srIsolated)
			require.NoError(t, err)
			return opB
		}
		opA := registerLocal1()
		opB := registerLocal2()
		oasA := opA.BuildOpenAPIOperation(srIsolated)
		oasB := opB.BuildOpenAPIOperation(srIsolated)
		refA := oasA.RequestBody.Content["application/json"].Schema.Ref
		refB := oasB.RequestBody.Content["multipart/form-data"].Schema.Ref
		assert.NotEqual(t, refA, refB)
		schemas := srIsolated.Schemas()
		assert.Contains(t, schemas[strings.TrimPrefix(refA, "#/components/schemas/")].Properties, "a")
		assert.Contains(t, schemas[strings.TrimPrefix(refB, "#/components/schemas/")].Properties, "b")
	})
}

func TestResponseFileAndXMLContracts(t *testing.T) {
	t.Run("compile_time_produces_and_status_rules", func(t *testing.T) {
		type jsonWithProduces struct {
			pi.Meta `path:"/j" method:"GET" produces:"application/json"`
		}
		type file204 struct {
			pi.Meta `path:"/f" method:"GET" status:"204"`
		}
		type xmlNullable struct {
			pi.Meta `path:"/x" method:"GET" nullable:"true"`
		}
		type badProducesSlashless struct {
			pi.Meta `path:"/f" method:"GET" produces:"png"`
		}
		type badProducesPrefixWildcard struct {
			pi.Meta `path:"/f" method:"GET" produces:"*/png"`
		}
		type badProducesPartialWildcard struct {
			pi.Meta `path:"/f" method:"GET" produces:"image/*png"`
		}
		type badProducesWildcardParams struct {
			pi.Meta `path:"/f" method:"GET" produces:"image/*; q=0.8"`
		}

		for _, tc := range []struct {
			req reflect.Type
			res reflect.Type
		}{
			{reflect.TypeOf((*jsonWithProduces)(nil)), reflect.TypeOf((*simpleRes)(nil))},
			{reflect.TypeOf((*file204)(nil)), reflect.TypeOf((*resTypes.File)(nil))},
			{reflect.TypeOf((*xmlNullable)(nil)), reflect.TypeOf((*resTypes.XML)(nil))},
			{reflect.TypeOf((*badProducesSlashless)(nil)), reflect.TypeOf((*resTypes.File)(nil))},
			{reflect.TypeOf((*badProducesPrefixWildcard)(nil)), reflect.TypeOf((*resTypes.File)(nil))},
			{reflect.TypeOf((*badProducesPartialWildcard)(nil)), reflect.TypeOf((*resTypes.File)(nil))},
			{reflect.TypeOf((*badProducesWildcardParams)(nil)), reflect.TypeOf((*resTypes.File)(nil))},
		} {
			_, err := contract.CompileOperation("h", tc.req, tc.res, contract.CompileOptions{}, contract.NewSchemaRegistry())
			assert.Error(t, err)
		}
	})

	t.Run("file_wrap_response_and_openapi", func(t *testing.T) {
		type imgReq struct {
			pi.Meta `summary:"Serve image" produces:"image/*, image/PNG, image/png"`
			ID      string `path:"id"`
		}
		op, sr := compileOp[imgReq, resTypes.File](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/uploads/{id}",
		})
		assert.Equal(t, contract.ResponseKindFile, op.ResponseKind)
		assert.Equal(t, []string{"image/*", "image/png"}, op.Produces)
		assert.Empty(t, sr.Schemas(), "response.File must never leak into components.schemas")

		oasOp := op.BuildOpenAPIOperation(sr)
		resp200 := oasOp.Responses["200"]
		require.NotNil(t, resp200)
		assert.Contains(t, resp200.Content, "image/*")
		assert.Contains(t, resp200.Content, "image/png")
		assert.Equal(t, "string", resp200.Content["image/*"].Schema.Type)
		assert.Equal(t, "binary", resp200.Content["image/*"].Schema.Format)
		assert.Empty(t, sr.Schemas())

		// Shared pointer must not be mutated; returns dereferenced value inside Result.
		shared := &resTypes.File{Content: []byte("img"), ContentType: "image/webp"}
		wrapped, err := op.WrapResponse(shared, nil)
		require.NoError(t, err)
		assert.Equal(t, 0, shared.StatusCode)
		res, ok := wrapped.(resTypes.Result)
		require.True(t, ok)
		assert.Equal(t, 200, res.StatusCode)
		fileVal, ok := res.Data.(resTypes.File)
		require.True(t, ok)
		assert.Equal(t, 200, fileVal.StatusCode)
		assert.Equal(t, "image/webp", fileVal.ContentType)

		// Headers["Content-Type"] preserved and synced to ContentType when ContentType is empty/whitespace.
		wrappedHdr, err := op.WrapResponse(&resTypes.File{
			Content:     []byte("png"),
			ContentType: "   ",
			Headers:     map[string]string{"content-type": "image/png"},
		}, nil)
		require.NoError(t, err)
		assert.Equal(t, "image/png", wrappedHdr.(resTypes.Result).Data.(resTypes.File).ContentType)
		assert.Equal(t, "image/png", wrappedHdr.(resTypes.Result).Data.(resTypes.File).Headers["content-type"])

		// Mixed-case Headers["content-type"] violating produces:"image/*" must fail with 500.
		_, err = op.WrapResponse(&resTypes.File{
			Content: []byte("<html/>"),
			Headers: map[string]string{"content-type": "text/html"},
		}, nil)
		require.Error(t, err)

		// Conflicting mixed-case Content-Type headers must fail with 500.
		_, err = op.WrapResponse(&resTypes.File{
			Content: []byte("png"),
			Headers: map[string]string{
				"Content-Type": "image/png",
				"content-type": "image/webp",
			},
		}, nil)
		require.Error(t, err)

		// Empty ContentType with produces:"image/*, image/png" (first range is wildcard) must fail per §4.6.3.
		_, err = op.WrapResponse(&resTypes.File{Content: []byte("x")}, nil)
		require.Error(t, err)

		// Mismatched ContentType against explicit produces:"image/*" fails with 500.
		_, err = op.WrapResponse(&resTypes.File{Content: []byte("<html/>"), ContentType: "text/html"}, nil)
		require.Error(t, err)

		// Mismatched StatusCode fails with 500.
		_, err = op.WrapResponse(&resTypes.File{Content: []byte("x"), ContentType: "image/png", StatusCode: 201}, nil)
		require.Error(t, err)

		// Wildcard produces:"image/*" with empty ContentType fails (cannot default to application/octet-stream).
		type wildcardOnlyReq struct {
			pi.Meta `produces:"image/*"`
		}
		opWild, _ := compileOp[wildcardOnlyReq, resTypes.File](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/wild",
		})
		_, err = opWild.WrapResponse(&resTypes.File{Content: []byte("x")}, nil)
		require.Error(t, err)

		// Default File (no produces) defaults empty ContentType to application/octet-stream.
		type defaultFileReq struct{}
		opDef, _ := compileOp[defaultFileReq, resTypes.File](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/dl",
		})
		wrappedDef, err := opDef.WrapResponse(&resTypes.File{Content: []byte("x")}, nil)
		require.NoError(t, err)
		assert.Equal(t, "application/octet-stream", wrappedDef.(resTypes.Result).Data.(resTypes.File).ContentType)
	})

	t.Run("xml_wrap_response_and_openapi", func(t *testing.T) {
		type rssReq struct {
			pi.Meta `produces:"application/rss+xml"`
		}
		op, sr := compileOp[rssReq, resTypes.XML](t, contract.CompileOptions{
			HasExplicitRoute: true,
			ExplicitMethod:   http.MethodGet,
			ExplicitPattern:  "/rss.xml",
		})
		assert.Equal(t, contract.ResponseKindXML, op.ResponseKind)
		assert.Equal(t, []string{"application/rss+xml"}, op.Produces)
		assert.Empty(t, sr.Schemas())

		oasOp := op.BuildOpenAPIOperation(sr)
		mt := oasOp.Responses["200"].Content["application/rss+xml"]
		require.NotNil(t, mt)
		assert.Equal(t, "string", mt.Schema.Type)
		assert.Empty(t, sr.Schemas())

		// ContentType with charset parameter matches produces:"application/rss+xml".
		wrapped, err := op.WrapResponse(&resTypes.XML{
			Content:     []byte("<rss/>"),
			ContentType: "application/rss+xml; charset=utf-8",
		}, nil)
		require.NoError(t, err)
		xmlVal, ok := wrapped.(resTypes.Result).Data.(resTypes.XML)
		require.True(t, ok)
		assert.Equal(t, "application/rss+xml; charset=utf-8", xmlVal.ContentType)

		// Empty ContentType fills concrete produces[0].
		wrappedEmpty, err := op.WrapResponse(&resTypes.XML{Content: []byte("<rss/>")}, nil)
		require.NoError(t, err)
		assert.Equal(t, "application/rss+xml", wrappedEmpty.(resTypes.Result).Data.(resTypes.XML).ContentType)
	})

	t.Run("anonymous_and_ignored_response_package_structs_rejected", func(t *testing.T) {
		type okReq struct{}
		type anonHandledRes struct{ resTypes.Handled }
		type anonPtrHandledRes struct{ *resTypes.Handled }
		type anonFileRes struct{ resTypes.File }
		type ignoredHandledRes struct {
			H *resTypes.Handled `json:"-"`
		}
		type anonFileReq struct{ resTypes.File }
		type anonPtrHandledReq struct{ *resTypes.Handled }
		type ignoredFileReq struct {
			F *resTypes.File `json:"-"`
		}
		type nestedAnonHandledReq struct {
			Inner struct {
				*resTypes.Handled
			} `json:"inner"`
		}
		type nestedIgnoredXMLReq struct {
			Inner struct {
				X *resTypes.XML `json:"-"`
			} `json:"inner"`
		}

		for _, resT := range []reflect.Type{
			reflect.TypeOf((*anonHandledRes)(nil)),
			reflect.TypeOf((*anonPtrHandledRes)(nil)),
			reflect.TypeOf((*anonFileRes)(nil)),
			reflect.TypeOf((*ignoredHandledRes)(nil)),
		} {
			_, err := contract.CompileOperation("h", reflect.TypeOf((*okReq)(nil)), resT, contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   http.MethodGet,
				ExplicitPattern:  "/bad-res",
			}, contract.NewSchemaRegistry())
			assert.Error(t, err, "expected compile error for response type %v", resT)
		}

		for _, reqT := range []reflect.Type{
			reflect.TypeOf((*anonFileReq)(nil)),
			reflect.TypeOf((*anonPtrHandledReq)(nil)),
			reflect.TypeOf((*ignoredFileReq)(nil)),
			reflect.TypeOf((*nestedAnonHandledReq)(nil)),
			reflect.TypeOf((*nestedIgnoredXMLReq)(nil)),
		} {
			_, err := contract.CompileOperation("h", reqT, reflect.TypeOf((*emptyRes)(nil)), contract.CompileOptions{
				HasExplicitRoute: true,
				ExplicitMethod:   http.MethodPost,
				ExplicitPattern:  "/bad-req",
			}, contract.NewSchemaRegistry())
			assert.Error(t, err, "expected compile error for request type %v", reqT)
		}
	})
}
