package openapiverify_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/http/response"
	"github.com/sllt/pi/pkg/pi/openapi"
	"github.com/sllt/pi/pkg/pi/testkit"
)

type verifyCustomList []int

func (c verifyCustomList) MarshalJSON() ([]byte, error) {
	return json.Marshal([]int(c))
}

type verifyPageRes[T any] struct {
	Items []T `json:"items"`
	Total int `json:"total"`
}

type verifyTreeNode struct {
	Name     string            `json:"name"`
	Children []*verifyTreeNode `json:"children,omitempty"`
}

func newVerifyApp(t *testing.T, extra ...pi.Option) *pi.App {
	t.Helper()
	opts := []pi.Option{
		pi.WithExplicitHTTPStatus(),
		pi.WithOpenAPI(pi.OpenAPIConfig{
			Title:   "OpenAPIVerify",
			Version: "1.0.0",
		}),
	}
	opts = append(opts, extra...)
	return testkit.New(t, opts...)
}

func loadAndValidateDoc(t *testing.T, data []byte) *openapi3.T {
	t.Helper()
	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	doc, err := loader.LoadFromData(data)
	if err != nil {
		t.Fatalf("LoadFromData failed: %v\nspec:\n%s", err, string(data))
	}
	if err := doc.Validate(context.Background()); err != nil {
		t.Fatalf("doc.Validate failed: %v\nspec:\n%s", err, string(data))
	}
	return doc
}

func operationFor(t *testing.T, doc *openapi3.T, routePath, method string) *openapi3.Operation {
	t.Helper()
	if doc == nil || doc.Paths == nil {
		t.Fatalf("missing paths in OpenAPI document")
	}
	piItem := doc.Paths.Value(routePath)
	if piItem == nil {
		t.Fatalf("missing OpenAPI path %q", routePath)
	}
	var op *openapi3.Operation
	switch strings.ToUpper(method) {
	case http.MethodGet:
		op = piItem.Get
	case http.MethodPost:
		op = piItem.Post
	case http.MethodPut:
		op = piItem.Put
	case http.MethodPatch:
		op = piItem.Patch
	case http.MethodDelete:
		op = piItem.Delete
	case http.MethodHead:
		op = piItem.Head
	case http.MethodOptions:
		op = piItem.Options
	}
	if op == nil {
		t.Fatalf("missing OpenAPI operation %s %s", method, routePath)
	}
	return op
}

func responseSchema(t *testing.T, doc *openapi3.T, routePath, method, status string) *openapi3.Schema {
	t.Helper()
	op := operationFor(t, doc, routePath, method)
	if op.Responses == nil {
		t.Fatalf("missing responses on %s %s", method, routePath)
	}
	respRef := op.Responses.Value(status)
	if respRef == nil || respRef.Value == nil {
		t.Fatalf("missing response %s on %s %s", status, method, routePath)
	}
	mt := respRef.Value.Content.Get("application/json")
	if mt == nil || mt.Schema == nil || mt.Schema.Value == nil {
		t.Fatalf("missing application/json schema on %s %s status %s", method, routePath, status)
	}
	return mt.Schema.Value
}

func requestBodySchema(t *testing.T, doc *openapi3.T, routePath, method string) *openapi3.Schema {
	t.Helper()
	op := operationFor(t, doc, routePath, method)
	if op.RequestBody == nil || op.RequestBody.Value == nil {
		t.Fatalf("missing requestBody on %s %s", method, routePath)
	}
	mt := op.RequestBody.Value.Content.Get("application/json")
	if mt == nil || mt.Schema == nil || mt.Schema.Value == nil {
		t.Fatalf("missing application/json requestBody schema on %s %s", method, routePath)
	}
	return mt.Schema.Value
}

func assertVisitJSON(t *testing.T, schema *openapi3.Schema, raw []byte) {
	t.Helper()
	var val any
	if err := json.Unmarshal(raw, &val); err != nil {
		t.Fatalf("json.Unmarshal failed: %v\nbody=%s", err, string(raw))
	}
	if err := schema.VisitJSON(val); err != nil {
		t.Fatalf("VisitJSON failed: %v\nbody=%s", err, string(raw))
	}
}

func assertVisitJSONFails(t *testing.T, schema *openapi3.Schema, raw []byte) {
	t.Helper()
	var val any
	if err := json.Unmarshal(raw, &val); err != nil {
		t.Fatalf("json.Unmarshal failed: %v\nbody=%s", err, string(raw))
	}
	if err := schema.VisitJSON(val); err == nil {
		t.Fatalf("expected VisitJSON failure for body=%s", string(raw))
	}
}

func postJSON(t *testing.T, app *pi.App, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	return testkit.Request(t, app, req)
}

func TestVerifyResponseNilCollections(t *testing.T) {
	type nilCollectionsReq struct {
		pi.Meta `path:"/nil-collections" method:"GET" tags:"verify"`
	}
	type nilCollectionsRes struct {
		Items  []int             `json:"items"`
		Labels map[string]string `json:"labels"`
		Raw    []byte            `json:"raw"`
	}

	app := newVerifyApp(t)
	if err := pi.Register(app, func(ctx *pi.Context, req *nilCollectionsReq) (*nilCollectionsRes, error) {
		return &nilCollectionsRes{}, nil
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)

	rec := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nil-collections", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	respSchema := responseSchema(t, doc, "/nil-collections", http.MethodGet, "200")
	assertVisitJSON(t, respSchema, rec.Body.Bytes())
}

func TestVerifyNullableObject(t *testing.T) {
	type nullableObjReq struct {
		pi.Meta `path:"/nullable-obj" method:"GET" tags:"verify" nullable:"true"`
		Empty   bool `query:"empty"`
	}
	type nullableObjOut struct {
		Name string `json:"name"`
	}

	app := newVerifyApp(t)
	if err := pi.Register(app, func(ctx *pi.Context, req *nullableObjReq) (*nullableObjOut, error) {
		if req.Empty {
			return nil, nil
		}
		return &nullableObjOut{Name: "alice"}, nil
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)
	respSchema := responseSchema(t, doc, "/nullable-obj", http.MethodGet, "200")

	recNil := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nullable-obj?empty=true", nil))
	if recNil.Code != http.StatusOK {
		t.Fatalf("nil response status=%d body=%s", recNil.Code, recNil.Body.String())
	}
	assertVisitJSON(t, respSchema, recNil.Body.Bytes())

	recNonNil := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nullable-obj?empty=false", nil))
	if recNonNil.Code != http.StatusOK {
		t.Fatalf("non-nil response status=%d body=%s", recNonNil.Code, recNonNil.Body.String())
	}
	assertVisitJSON(t, respSchema, recNonNil.Body.Bytes())
}

func TestVerifyNestedNullableObject(t *testing.T) {
	type childStruct struct {
		Label string `json:"label"`
	}
	type parentRes struct {
		Title string       `json:"title"`
		Child *childStruct `json:"child"`
	}
	type nestedReq struct {
		pi.Meta   `path:"/nested-nullable" method:"GET" tags:"verify"`
		WithChild bool `query:"withChild"`
	}

	app := newVerifyApp(t)
	if err := pi.Register(app, func(ctx *pi.Context, req *nestedReq) (*parentRes, error) {
		res := &parentRes{Title: "root"}
		if req.WithChild {
			res.Child = &childStruct{Label: "leaf"}
		}
		return res, nil
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)
	respSchema := responseSchema(t, doc, "/nested-nullable", http.MethodGet, "200")

	recNilChild := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nested-nullable?withChild=false", nil))
	if recNilChild.Code != http.StatusOK {
		t.Fatalf("nil child status=%d body=%s", recNilChild.Code, recNilChild.Body.String())
	}
	assertVisitJSON(t, respSchema, recNilChild.Body.Bytes())

	recWithChild := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/nested-nullable?withChild=true", nil))
	if recWithChild.Code != http.StatusOK {
		t.Fatalf("with child status=%d body=%s", recWithChild.Code, recWithChild.Body.String())
	}
	assertVisitJSON(t, respSchema, recWithChild.Body.Bytes())
}

func TestVerifyGuardedInvalidValidation(t *testing.T) {
	type badValidateReq struct {
		pi.Meta `path:"/bad-validate" method:"POST" tags:"verify"`
		State   string `json:"state" validate:"eq=ready,min=oops"`
	}
	type ackRes struct {
		OK bool `json:"ok"`
	}

	app := newVerifyApp(t)
	err := pi.Register(app, func(ctx *pi.Context, req *badValidateReq) (*ackRes, error) {
		return &ackRes{OK: true}, nil
	})
	if err == nil {
		t.Fatalf("expected Register to fail on invalid guarded validate tag")
	}
	if app.RouteError() == nil {
		t.Fatalf("expected non-nil app.RouteError()")
	}
}

func TestVerifyCustomCollectionMarshaler(t *testing.T) {
	type customMarshalerReq struct {
		pi.Meta `path:"/custom-marshaler" method:"GET" tags:"verify"`
	}
	type customMarshalerRes struct {
		Items verifyCustomList `json:"items"`
	}

	app := newVerifyApp(t)
	err := pi.Register(app, func(ctx *pi.Context, req *customMarshalerReq) (*customMarshalerRes, error) {
		return &customMarshalerRes{Items: verifyCustomList{1, 2}}, nil
	})
	if err == nil {
		t.Fatalf("expected Register to reject custom collection type implementing json.Marshaler")
	}
}

func TestVerifyCrossMethodParameterNames(t *testing.T) {
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
	type userRes struct {
		ID string `json:"id"`
	}

	t.Run("mismatched parameter names across methods are rejected", func(t *testing.T) {
		app := newVerifyApp(t)
		if err := pi.Register(app, func(ctx *pi.Context, req *getUserByIDReq) (*userRes, error) {
			return &userRes{ID: req.ID}, nil
		}); err != nil {
			t.Fatalf("first Register failed: %v", err)
		}

		if err := pi.Register(app, func(ctx *pi.Context, req *postUserByNameReq) (*userRes, error) {
			return &userRes{ID: req.Name}, nil
		}); err == nil {
			t.Fatalf("expected Register with mismatched path parameter name to fail")
		}
		if app.RouteError() == nil {
			t.Fatalf("expected app.RouteError() != nil")
		}
	})

	t.Run("consistent parameter names across methods pass kin-openapi Validate", func(t *testing.T) {
		app := newVerifyApp(t)
		if err := pi.Register(app, func(ctx *pi.Context, req *getUserByIDReq) (*userRes, error) {
			return &userRes{ID: req.ID}, nil
		}); err != nil {
			t.Fatalf("GET Register failed: %v", err)
		}
		if err := pi.Register(app, func(ctx *pi.Context, req *postUserByIDReq) (*userRes, error) {
			return &userRes{ID: req.ID}, nil
		}); err != nil {
			t.Fatalf("POST Register failed: %v", err)
		}

		specBytes, err := app.OpenAPIJSON()
		if err != nil {
			t.Fatalf("OpenAPIJSON failed: %v", err)
		}
		doc := loadAndValidateDoc(t, specBytes)
		item := doc.Paths.Value("/users/{id}")
		if item == nil || item.Get == nil || item.Post == nil {
			t.Fatalf("expected GET and POST under /users/{id}")
		}
	})
}

func TestVerifyParameterGroupMountCollision(t *testing.T) {
	type profileReq struct {
		pi.Meta `path:"/profile" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type settingsReq struct {
		pi.Meta `path:"/settings" method:"GET" tags:"users"`
		Name    string `path:"name" validate:"required"`
	}
	type okRes struct {
		Value string `json:"value"`
	}

	app := newVerifyApp(t)
	idGroup := app.Group("/users/{id}")
	nameGroup := app.Group("/users/{name}")

	if err := pi.Register(idGroup, func(ctx *pi.Context, req *profileReq) (*okRes, error) {
		return &okRes{Value: req.ID}, nil
	}); err != nil {
		t.Fatalf("first group Register failed: %v", err)
	}

	if err := pi.Register(nameGroup, func(ctx *pi.Context, req *settingsReq) (*okRes, error) {
		return &okRes{Value: req.Name}, nil
	}); err == nil {
		t.Fatalf("expected second conflicting parameter group Register to fail")
	}

	if app.RouteError() == nil {
		t.Fatalf("expected non-nil RouteError after group mount collision")
	}
	if h, err := app.HTTPHandler(); err == nil || h != nil {
		t.Fatalf("expected HTTPHandler to return error without panic, got h=%v err=%v", h, err)
	}
}

func TestVerifyFixedArrayLength(t *testing.T) {
	type fixedArrayReq struct {
		pi.Meta `path:"/fixed-array" method:"POST" tags:"verify"`
		Pair    [2]int `json:"pair" validate:"required"`
	}
	type fixedArrayRes struct {
		Sum int `json:"sum"`
	}

	app := newVerifyApp(t)
	if err := pi.Register(app, func(ctx *pi.Context, req *fixedArrayReq) (*fixedArrayRes, error) {
		return &fixedArrayRes{Sum: req.Pair[0] + req.Pair[1]}, nil
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)
	reqSchema := requestBodySchema(t, doc, "/fixed-array", http.MethodPost)
	respSchema := responseSchema(t, doc, "/fixed-array", http.MethodPost, "200")

	for _, badBody := range []string{`{"pair":[1]}`, `{"pair":[1,2,3]}`} {
		assertVisitJSONFails(t, reqSchema, []byte(badBody))
		rec := postJSON(t, app, "/fixed-array", badBody)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got status=%d body=%s", badBody, rec.Code, rec.Body.String())
		}
	}

	goodBody := `{"pair":[1,2]}`
	assertVisitJSON(t, reqSchema, []byte(goodBody))
	recOK := postJSON(t, app, "/fixed-array", goodBody)
	if recOK.Code != http.StatusOK {
		t.Fatalf("expected 200 for %s, got status=%d body=%s", goodBody, recOK.Code, recOK.Body.String())
	}
	assertVisitJSON(t, respSchema, recOK.Body.Bytes())
}

func TestVerifySpecPathProbeCollision(t *testing.T) {
	_, err := pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
		Title:     "ProbeCollision",
		Version:   "1.0.0",
		ServeSpec: true,
		SpecPath:  "/.well-known/ready",
	}))
	if err == nil {
		t.Fatalf("expected pi.Build to reject SpecPath colliding with system probe")
	}

	_, err = pi.Build(pi.WithOpenAPI(pi.OpenAPIConfig{
		Title:     "UIProbeCollision",
		Version:   "1.0.0",
		ServeSpec: true,
		ServeUI:   true,
		UIPath:    "/.well-known/health",
	}))
	if err == nil {
		t.Fatalf("expected pi.Build to reject UIPath colliding with system probe")
	}
}

func TestVerifyDiveRequiredScope(t *testing.T) {
	type diveReq struct {
		pi.Meta `path:"/dive-required" method:"POST" tags:"verify"`
		Items   []string `json:"items" validate:"dive,required"`
	}
	type diveRes struct {
		Count int `json:"count"`
	}

	app := newVerifyApp(t)
	if err := pi.Register(app, func(ctx *pi.Context, req *diveReq) (*diveRes, error) {
		return &diveRes{Count: len(req.Items)}, nil
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)
	reqSchema := requestBodySchema(t, doc, "/dive-required", http.MethodPost)
	respSchema := responseSchema(t, doc, "/dive-required", http.MethodPost, "200")

	for _, validBody := range []string{`{}`, `{"items":["ok"]}`} {
		assertVisitJSON(t, reqSchema, []byte(validBody))
		rec := postJSON(t, app, "/dive-required", validBody)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200 for %s, got status=%d body=%s", validBody, rec.Code, rec.Body.String())
		}
		assertVisitJSON(t, respSchema, rec.Body.Bytes())
	}

	invalidBody := `{"items":[""]}`
	assertVisitJSONFails(t, reqSchema, []byte(invalidBody))
	recInvalid := postJSON(t, app, "/dive-required", invalidBody)
	if recInvalid.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for %s, got status=%d body=%s", invalidBody, recInvalid.Code, recInvalid.Body.String())
	}
}

func TestVerifyYAMLAndJSONSpecParity(t *testing.T) {
	type itemDTO struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	type listItemsReq struct {
		pi.Meta `path:"/items" method:"GET" tags:"items" security:"bearerAuth"`
		States  []string `query:"states" validate:"dive,oneof=open closed"`
	}
	type getTreeReq struct {
		pi.Meta `path:"/tree" method:"GET" tags:"tree"`
	}

	app := newVerifyApp(t, pi.WithOpenAPI(pi.OpenAPIConfig{
		Title:       "ParityAPI",
		Version:     "2.0.0",
		Description: "JSON and YAML kin-openapi parity check",
		Servers:     []openapi.Server{{URL: "https://api.example.com", Description: "Prod"}},
		ServeSpec:   true,
		SecuritySchemes: map[string]openapi.SecurityScheme{
			"bearerAuth": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
		},
	}))

	if err := pi.Register(app, func(ctx *pi.Context, req *listItemsReq) (*verifyPageRes[itemDTO], error) {
		return &verifyPageRes[itemDTO]{
			Items: []itemDTO{{ID: "1", Name: "alpha"}},
			Total: 1,
		}, nil
	}); err != nil {
		t.Fatalf("Register /items failed: %v", err)
	}

	if err := pi.Register(app, func(ctx *pi.Context, req *getTreeReq) (*verifyTreeNode, error) {
		return &verifyTreeNode{
			Name:     "root",
			Children: []*verifyTreeNode{{Name: "leaf"}},
		}, nil
	}); err != nil {
		t.Fatalf("Register /tree failed: %v", err)
	}

	jsonBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	yamlBytes, err := app.OpenAPIYAML()
	if err != nil {
		t.Fatalf("OpenAPIYAML failed: %v", err)
	}

	jsonDoc := loadAndValidateDoc(t, jsonBytes)
	yamlDoc := loadAndValidateDoc(t, yamlBytes)

	for _, doc := range []*openapi3.T{jsonDoc, yamlDoc} {
		if doc.Components == nil || doc.Components.SecuritySchemes["bearerAuth"] == nil {
			t.Fatalf("expected bearerAuth security scheme in OpenAPI components")
		}
		itemsOp := operationFor(t, doc, "/items", http.MethodGet)
		if itemsOp.Security == nil || len(*itemsOp.Security) == 0 {
			t.Fatalf("expected security requirement on GET /items")
		}
		var foundStates bool
		for _, param := range itemsOp.Parameters {
			if param != nil && param.Value != nil && param.Value.Name == "states" && param.Value.In == "query" {
				foundStates = true
				if param.Value.Schema == nil || param.Value.Schema.Value == nil || param.Value.Schema.Value.Items == nil || param.Value.Schema.Value.Items.Value == nil {
					t.Fatalf("expected query slice items schema on states")
				}
				if len(param.Value.Schema.Value.Items.Value.Enum) != 2 {
					t.Fatalf("expected 2 enum values on states items, got %v", param.Value.Schema.Value.Items.Value.Enum)
				}
			}
		}
		if !foundStates {
			t.Fatalf("expected query parameter states on GET /items")
		}
		if operationFor(t, doc, "/tree", http.MethodGet) == nil {
			t.Fatalf("expected GET /tree operation")
		}
	}

	recItems := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/items?states=open", nil))
	if recItems.Code != http.StatusOK {
		t.Fatalf("GET /items status=%d body=%s", recItems.Code, recItems.Body.String())
	}
	assertVisitJSON(t, responseSchema(t, jsonDoc, "/items", http.MethodGet, "200"), recItems.Body.Bytes())
	assertVisitJSON(t, responseSchema(t, yamlDoc, "/items", http.MethodGet, "200"), recItems.Body.Bytes())

	recTree := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/tree", nil))
	if recTree.Code != http.StatusOK {
		t.Fatalf("GET /tree status=%d body=%s", recTree.Code, recTree.Body.String())
	}
	assertVisitJSON(t, responseSchema(t, jsonDoc, "/tree", http.MethodGet, "200"), recTree.Body.Bytes())
	assertVisitJSON(t, responseSchema(t, yamlDoc, "/tree", http.MethodGet, "200"), recTree.Body.Bytes())
}

func TestVerifyLegacyRegexpCompatibility(t *testing.T) {
	app := newVerifyApp(t)
	app.GET("/users/{id:[0-9]+}", func(ctx *pi.Context) (any, error) {
		return map[string]string{"id": ctx.PathParam("id")}, nil
	})
	app.GET("/users/{slug:[a-z]+}", func(ctx *pi.Context) (any, error) {
		return map[string]string{"slug": ctx.PathParam("slug")}, nil
	})

	if err := app.RouteError(); err != nil {
		t.Fatalf("RouteError failed: %v", err)
	}
	h, err := app.HTTPHandler()
	if err != nil {
		t.Fatalf("HTTPHandler failed: %v", err)
	}

	recID := httptest.NewRecorder()
	h.ServeHTTP(recID, httptest.NewRequest(http.MethodGet, "/users/123", nil))
	if recID.Code != http.StatusOK || !strings.Contains(recID.Body.String(), `"123"`) {
		t.Fatalf("GET /users/123 status=%d body=%s", recID.Code, recID.Body.String())
	}

	recSlug := httptest.NewRecorder()
	h.ServeHTTP(recSlug, httptest.NewRequest(http.MethodGet, "/users/alice", nil))
	if recSlug.Code != http.StatusOK || !strings.Contains(recSlug.Body.String(), `"alice"`) {
		t.Fatalf("GET /users/alice status=%d body=%s", recSlug.Code, recSlug.Body.String())
	}

	recMiss := httptest.NewRecorder()
	h.ServeHTTP(recMiss, httptest.NewRequest(http.MethodGet, "/users/a1b2", nil))
	if recMiss.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for /users/a1b2, got status=%d", recMiss.Code)
	}
}

func TestVerifyNestedAndFlatGroupCoexistence(t *testing.T) {
	type nestedProfileReq struct {
		pi.Meta `path:"/profile" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type flatSettingsReq struct {
		pi.Meta `path:"/settings" method:"GET" tags:"users"`
		ID      string `path:"id" validate:"required"`
	}
	type ordersReq struct {
		pi.Meta `path:"/v1/orders" method:"GET" tags:"orders"`
	}
	type usersReq struct {
		pi.Meta `path:"/users" method:"GET" tags:"users"`
	}
	type strOut struct {
		Value string `json:"value"`
	}

	app := newVerifyApp(t)

	nestedIDGroup := app.Group("/users").Group("/{id}")
	if err := pi.Register(nestedIDGroup, func(ctx *pi.Context, req *nestedProfileReq) (*strOut, error) {
		return &strOut{Value: "profile:" + req.ID}, nil
	}); err != nil {
		t.Fatalf("Register nested failed: %v", err)
	}

	flatIDGroup := app.Group("/users/{id}")
	if err := pi.Register(flatIDGroup, func(ctx *pi.Context, req *flatSettingsReq) (*strOut, error) {
		return &strOut{Value: "settings:" + req.ID}, nil
	}); err != nil {
		t.Fatalf("Register flat failed: %v", err)
	}

	if err := pi.Register(app.Group("/api"), func(ctx *pi.Context, _ *ordersReq) (*strOut, error) {
		return &strOut{Value: "orders"}, nil
	}); err != nil {
		t.Fatalf("Register /api orders failed: %v", err)
	}
	if err := pi.Register(app.Group("/api/v1"), func(ctx *pi.Context, _ *usersReq) (*strOut, error) {
		return &strOut{Value: "users"}, nil
	}); err != nil {
		t.Fatalf("Register /api/v1 users failed: %v", err)
	}

	if err := app.RouteError(); err != nil {
		t.Fatalf("RouteError failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)

	h, err := app.HTTPHandler()
	if err != nil {
		t.Fatalf("HTTPHandler failed: %v", err)
	}

	for _, tc := range []struct {
		specPath string
		reqPath  string
		wantVal  string
	}{
		{specPath: "/users/{id}/profile", reqPath: "/users/42/profile", wantVal: `"profile:42"`},
		{specPath: "/users/{id}/settings", reqPath: "/users/99/settings", wantVal: `"settings:99"`},
		{specPath: "/api/v1/orders", reqPath: "/api/v1/orders", wantVal: `"orders"`},
		{specPath: "/api/v1/users", reqPath: "/api/v1/users", wantVal: `"users"`},
	} {
		_ = operationFor(t, doc, tc.specPath, http.MethodGet)
		respSchema := responseSchema(t, doc, tc.specPath, http.MethodGet, "200")

		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.reqPath, nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), tc.wantVal) {
			t.Fatalf("GET %s status=%d body=%s (want %s)", tc.reqPath, rec.Code, rec.Body.String(), tc.wantVal)
		}
		assertVisitJSON(t, respSchema, rec.Body.Bytes())
	}
}

func TestVerifyConditionalCrossFieldInvalidLiteral(t *testing.T) {
	type dummyOut struct {
		OK bool `json:"ok"`
	}

	t.Run("required_if invalid int literal is rejected at Register", func(t *testing.T) {
		type badReq struct {
			pi.Meta `path:"/cond-req-if" method:"POST" tags:"verify"`
			Mode    string `json:"mode"`
			Count   int    `json:"count"`
			Target  string `json:"target" validate:"required_if=Mode ready Count oops"`
		}
		app := newVerifyApp(t)
		err := pi.Register(app, func(ctx *pi.Context, req *badReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		})
		if err == nil {
			t.Fatalf("expected Register error for invalid required_if int literal")
		}
	})

	t.Run("excluded_if invalid int literal is rejected at Register", func(t *testing.T) {
		type badReq struct {
			pi.Meta `path:"/cond-excl-if" method:"POST" tags:"verify"`
			Mode    string `json:"mode"`
			Count   int    `json:"count"`
			Target  string `json:"target" validate:"excluded_if=Mode ready Count oops"`
		}
		app := newVerifyApp(t)
		err := pi.Register(app, func(ctx *pi.Context, req *badReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		})
		if err == nil {
			t.Fatalf("expected Register error for invalid excluded_if int literal")
		}
	})

	t.Run("required_unless invalid float literal is rejected at Register", func(t *testing.T) {
		type badReq struct {
			pi.Meta `path:"/cond-req-unless" method:"POST" tags:"verify"`
			Mode    string  `json:"mode"`
			Ratio   float64 `json:"ratio"`
			Target  string  `json:"target" validate:"required_unless=Mode ready Ratio notfloat"`
		}
		app := newVerifyApp(t)
		err := pi.Register(app, func(ctx *pi.Context, req *badReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		})
		if err == nil {
			t.Fatalf("expected Register error for invalid required_unless float literal")
		}
	})

	t.Run("skip_unless invalid uint literal is rejected at Register", func(t *testing.T) {
		type badReq struct {
			pi.Meta `path:"/cond-skip-unless" method:"POST" tags:"verify"`
			Mode    string `json:"mode"`
			UCount  uint   `json:"uCount"`
			Target  string `json:"target" validate:"skip_unless=Mode ready UCount -5"`
		}
		app := newVerifyApp(t)
		err := pi.Register(app, func(ctx *pi.Context, req *badReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		})
		if err == nil {
			t.Fatalf("expected Register error for invalid skip_unless uint literal")
		}
	})

	t.Run("duplicate field in required_if is rejected at Register", func(t *testing.T) {
		type badReq struct {
			pi.Meta `path:"/cond-dup" method:"POST" tags:"verify"`
			Mode    string `json:"mode"`
			Target  string `json:"target" validate:"required_if=Mode a Mode b"`
		}
		app := newVerifyApp(t)
		err := pi.Register(app, func(ctx *pi.Context, req *badReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		})
		if err == nil {
			t.Fatalf("expected Register error for duplicate field in required_if")
		}
	})

	t.Run("pointer field with skip_unless followed by min without omit modifier is rejected at Register", func(t *testing.T) {
		type badReq struct {
			pi.Meta `path:"/cond-ptr-panic" method:"POST" tags:"verify"`
			Mode    string  `json:"mode"`
			Target  *string `json:"target" validate:"skip_unless=Mode strict,min=3"`
		}
		app := newVerifyApp(t)
		err := pi.Register(app, func(ctx *pi.Context, req *badReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		})
		if err == nil {
			t.Fatalf("expected Register error for pointer skip_unless followed by min without omit modifier")
		}
	})

	t.Run("valid multi-field conditional rule validates at runtime without panic", func(t *testing.T) {
		type validReq struct {
			pi.Meta `path:"/cond-valid" method:"POST" tags:"verify"`
			Mode    string `json:"mode"`
			Count   int    `json:"count"`
			Note    string `json:"note" validate:"required_if=Mode ready Count 2"`
		}
		app := newVerifyApp(t)
		if err := pi.Register(app, func(ctx *pi.Context, req *validReq) (*dummyOut, error) {
			return &dummyOut{OK: true}, nil
		}); err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		specBytes, err := app.OpenAPIJSON()
		if err != nil {
			t.Fatalf("OpenAPIJSON failed: %v", err)
		}
		doc := loadAndValidateDoc(t, specBytes)
		respSchema := responseSchema(t, doc, "/cond-valid", http.MethodPost, "200")

		recBad := postJSON(t, app, "/cond-valid", `{"mode":"ready","count":2,"note":""}`)
		if recBad.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 when Mode=ready & Count=2 & Note='', got status=%d body=%s", recBad.Code, recBad.Body.String())
		}

		for _, okBody := range []string{
			`{"mode":"ready","count":2,"note":"ok"}`,
			`{"mode":"ready","count":1,"note":""}`,
		} {
			recOK := postJSON(t, app, "/cond-valid", okBody)
			if recOK.Code != http.StatusOK {
				t.Fatalf("expected 200 for %s, got status=%d body=%s", okBody, recOK.Code, recOK.Body.String())
			}
			assertVisitJSON(t, respSchema, recOK.Body.Bytes())
		}
	})
}

func TestVerifyDiveRequiredPointerElementsAndNullability(t *testing.T) {
	type divePtrItemsReq struct {
		pi.Meta `path:"/dive-ptr-items" method:"POST" tags:"verify"`
		Items   []*string `json:"items" validate:"dive,required"`
	}
	type nullabilityChildDTO struct {
		Name string `json:"name" validate:"required"`
	}
	type ptrNullabilityReq struct {
		pi.Meta     `path:"/ptr-nullability" method:"POST" tags:"verify"`
		ReqPtr      *string                `json:"reqPtr" validate:"required"`
		OptPtr      *string                `json:"optPtr"`
		Child       *nullabilityChildDTO   `json:"child" validate:"required"`
		PtrChildren []*nullabilityChildDTO `json:"ptrChildren" validate:"dive,required"`
		PtrMap      map[string]*string     `json:"ptrMap" validate:"dive,required"`
	}
	type ptrStructOnlyCondReq struct {
		pi.Meta          `path:"/ptr-structonly-cond" method:"POST" tags:"verify"`
		Mode             string               `json:"mode"`
		StrictStructOnly *nullabilityChildDTO `json:"strictStructOnly" validate:"structonly"`
		OmitStructOnly   *nullabilityChildDTO `json:"omitStructOnly" validate:"omitempty,structonly"`
		CondPtr          *string              `json:"condPtr" validate:"required_if=Mode strict,omitempty,min=3"`
	}
	type okOut struct {
		Count int `json:"count"`
	}

	app := newVerifyApp(t)
	if err := pi.Register(app, func(ctx *pi.Context, req *divePtrItemsReq) (*okOut, error) {
		return &okOut{Count: len(req.Items)}, nil
	}); err != nil {
		t.Fatalf("Register dive-ptr-items failed: %v", err)
	}
	if err := pi.Register(app, func(ctx *pi.Context, req *ptrNullabilityReq) (*okOut, error) {
		return &okOut{Count: len(req.PtrChildren) + len(req.PtrMap)}, nil
	}); err != nil {
		t.Fatalf("Register ptr-nullability failed: %v", err)
	}
	if err := pi.Register(app, func(ctx *pi.Context, req *ptrStructOnlyCondReq) (*okOut, error) {
		return &okOut{Count: 1}, nil
	}); err != nil {
		t.Fatalf("Register ptr-structonly-cond failed: %v", err)
	}

	specBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	doc := loadAndValidateDoc(t, specBytes)

	t.Run("slice of pointer strings with dive,required", func(t *testing.T) {
		reqSchema := requestBodySchema(t, doc, "/dive-ptr-items", http.MethodPost)
		respSchema := responseSchema(t, doc, "/dive-ptr-items", http.MethodPost, "200")

		for _, validBody := range []string{`{}`, `{"items":[""]}`, `{"items":["ok"]}`} {
			assertVisitJSON(t, reqSchema, []byte(validBody))
			rec := postJSON(t, app, "/dive-ptr-items", validBody)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200 for %s, got status=%d body=%s", validBody, rec.Code, rec.Body.String())
			}
			assertVisitJSON(t, respSchema, rec.Body.Bytes())
		}

		invalidBody := `{"items":[null]}`
		assertVisitJSONFails(t, reqSchema, []byte(invalidBody))
		recInvalid := postJSON(t, app, "/dive-ptr-items", invalidBody)
		if recInvalid.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for %s, got status=%d body=%s", invalidBody, recInvalid.Code, recInvalid.Body.String())
		}
	})

	t.Run("required pointer fields, pointer children, and map pointer values nullability parity", func(t *testing.T) {
		reqSchema := requestBodySchema(t, doc, "/ptr-nullability", http.MethodPost)
		respSchema := responseSchema(t, doc, "/ptr-nullability", http.MethodPost, "200")

		for _, validBody := range []string{
			`{"reqPtr":"","child":{"name":"ok"}}`,
			`{"reqPtr":"hi","optPtr":null,"child":{"name":"ok"},"ptrChildren":[{"name":"leaf"}],"ptrMap":{"a":"","b":"val"}}`,
		} {
			assertVisitJSON(t, reqSchema, []byte(validBody))
			rec := postJSON(t, app, "/ptr-nullability", validBody)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200 for %s, got status=%d body=%s", validBody, rec.Code, rec.Body.String())
			}
			assertVisitJSON(t, respSchema, rec.Body.Bytes())
		}

		for _, invalidBody := range []string{
			`{"optPtr":"x","child":{"name":"ok"}}`,
			`{"reqPtr":null,"child":{"name":"ok"}}`,
			`{"reqPtr":"ok","child":null}`,
			`{"reqPtr":"ok","child":{"name":"ok"},"ptrChildren":[null]}`,
			`{"reqPtr":"ok","child":{"name":"ok"},"ptrMap":{"k":null}}`,
		} {
			assertVisitJSONFails(t, reqSchema, []byte(invalidBody))
			rec := postJSON(t, app, "/ptr-nullability", invalidBody)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got status=%d body=%s", invalidBody, rec.Code, rec.Body.String())
			}
		}
	})

	t.Run("structonly, omitempty structonly, and required_if with omitempty min=3 pointer parity", func(t *testing.T) {
		reqSchema := requestBodySchema(t, doc, "/ptr-structonly-cond", http.MethodPost)
		respSchema := responseSchema(t, doc, "/ptr-structonly-cond", http.MethodPost, "200")

		for _, validBody := range []string{
			`{"mode":"relaxed","strictStructOnly":{"name":""},"omitStructOnly":null,"condPtr":null}`,
			`{"mode":"strict","strictStructOnly":{"name":"ok"},"omitStructOnly":{"name":""},"condPtr":"abc"}`,
		} {
			assertVisitJSON(t, reqSchema, []byte(validBody))
			rec := postJSON(t, app, "/ptr-structonly-cond", validBody)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200 for %s, got status=%d body=%s", validBody, rec.Code, rec.Body.String())
			}
			assertVisitJSON(t, respSchema, rec.Body.Bytes())
		}

		for _, schemaAndRuntimeInvalid := range []string{
			`{"mode":"relaxed","strictStructOnly":null,"omitStructOnly":null,"condPtr":null}`,
			`{"mode":"relaxed","strictStructOnly":{"name":"ok"},"omitStructOnly":null,"condPtr":"ab"}`,
			`{"mode":"strict","strictStructOnly":{"name":"ok"},"omitStructOnly":null,"condPtr":"ab"}`,
		} {
			assertVisitJSONFails(t, reqSchema, []byte(schemaAndRuntimeInvalid))
			rec := postJSON(t, app, "/ptr-structonly-cond", schemaAndRuntimeInvalid)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for %s, got status=%d body=%s", schemaAndRuntimeInvalid, rec.Code, rec.Body.String())
			}
		}

		strictMissingCond := `{"mode":"strict","strictStructOnly":{"name":"ok"},"omitStructOnly":null,"condPtr":null}`
		recStrictMissing := postJSON(t, app, "/ptr-structonly-cond", strictMissingCond)
		if recStrictMissing.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 for strict missing condPtr (%s), got status=%d body=%s", strictMissingCond, recStrictMissing.Code, recStrictMissing.Body.String())
		}
	})
}

func TestVerifyUnifiedRoutingMultipartFileXMLAndHeadOptions(t *testing.T) {
	type noMetaGetReq struct {
		ID string `path:"id" validate:"required"`
	}
	type noMetaRes struct {
		ID string `json:"id"`
	}
	type uploadReq struct {
		pi.Meta `status:"201" summary:"Upload media"`
		Title   string                `form:"title" validate:"required"`
		File    *multipart.FileHeader `form:"file" validate:"required"`
	}
	type uploadRes struct {
		Title    string `json:"title"`
		Filename string `json:"filename"`
		Size     int    `json:"size"`
	}
	type serveFileReq struct {
		pi.Meta `produces:"image/*" summary:"Serve media"`
		ID      string `path:"id" validate:"required"`
	}
	type rssReq struct {
		pi.Meta `produces:"application/rss+xml" summary:"RSS feed"`
	}

	app := newVerifyApp(t, pi.WithOpenAPI(pi.OpenAPIConfig{
		Title:   "UnifiedVerify",
		Version: "1.0.0",
		SecuritySchemes: map[string]openapi.SecurityScheme{
			"bearerAuth": {Type: "http", Scheme: "bearer", BearerFormat: "JWT"},
		},
	}))

	v1 := app.Group("/api/v1").WithDoc(pi.DocTags("unified"))
	authed := v1.Scope().WithDoc(pi.DocSecurity("bearerAuth"))

	v1.GET("/items/{id}", pi.H(func(ctx *pi.Context, req *noMetaGetReq) (*noMetaRes, error) {
		return &noMetaRes{ID: req.ID}, nil
	}))
	v1.HEAD("/items/{id}", func(ctx *pi.Context, req *noMetaGetReq) (*noMetaRes, error) {
		return &noMetaRes{ID: req.ID}, nil
	})
	v1.OPTIONS("/items/{id}", func(ctx *pi.Context, req *noMetaGetReq) (*noMetaRes, error) {
		return &noMetaRes{ID: req.ID}, nil
	})
	authed.POST("/media", func(ctx *pi.Context, req *uploadReq) (*uploadRes, error) {
		f, err := req.File.Open()
		if err != nil {
			return nil, err
		}
		defer f.Close()
		b, err := io.ReadAll(f)
		if err != nil {
			return nil, err
		}
		return &uploadRes{Title: req.Title, Filename: req.File.Filename, Size: len(b)}, nil
	})
	app.GET("/media/{id}", func(ctx *pi.Context, req *serveFileReq) (*response.File, error) {
		return &response.File{Content: []byte("PNG"), ContentType: "image/png"}, nil
	})
	app.GET("/rss.xml", func(ctx *pi.Context, req *rssReq) (*response.XML, error) {
		return &response.XML{Content: []byte("<rss/>")}, nil
	})

	if err := app.RouteError(); err != nil {
		t.Fatalf("RouteError failed: %v", err)
	}

	jsonBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	yamlBytes, err := app.OpenAPIYAML()
	if err != nil {
		t.Fatalf("OpenAPIYAML failed: %v", err)
	}

	jsonDoc := loadAndValidateDoc(t, jsonBytes)
	yamlDoc := loadAndValidateDoc(t, yamlBytes)

	for _, doc := range []*openapi3.T{jsonDoc, yamlDoc} {
		getOp := operationFor(t, doc, "/api/v1/items/{id}", http.MethodGet)
		if getOp.Security != nil && len(*getOp.Security) > 0 {
			t.Fatalf("expected GET /api/v1/items/{id} to have no security, got %v", *getOp.Security)
		}
		_ = operationFor(t, doc, "/api/v1/items/{id}", http.MethodHead)
		_ = operationFor(t, doc, "/api/v1/items/{id}", http.MethodOptions)

		uploadOp := operationFor(t, doc, "/api/v1/media", http.MethodPost)
		if uploadOp.Security == nil || len(*uploadOp.Security) == 0 {
			t.Fatalf("expected POST /api/v1/media in Scope to inherit Bearer security")
		}
		if uploadOp.RequestBody == nil || uploadOp.RequestBody.Value == nil || uploadOp.RequestBody.Value.Content.Get("multipart/form-data") == nil {
			t.Fatalf("expected multipart/form-data requestBody on POST /api/v1/media")
		}

		mediaOp := operationFor(t, doc, "/media/{id}", http.MethodGet)
		if resp200 := mediaOp.Responses.Value("200"); resp200 == nil || resp200.Value == nil || resp200.Value.Content.Get("image/*") == nil {
			t.Fatalf("expected image/* response content on GET /media/{id}")
		}
		rssOp := operationFor(t, doc, "/rss.xml", http.MethodGet)
		if resp200 := rssOp.Responses.Value("200"); resp200 == nil || resp200.Value == nil || resp200.Value.Content.Get("application/rss+xml") == nil {
			t.Fatalf("expected application/rss+xml response content on GET /rss.xml")
		}
	}

	// Runtime parity checks against validated schema.
	recGet := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/api/v1/items/42", nil))
	if recGet.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/items/42 status=%d body=%s", recGet.Code, recGet.Body.String())
	}
	assertVisitJSON(t, responseSchema(t, jsonDoc, "/api/v1/items/{id}", http.MethodGet, "200"), recGet.Body.Bytes())

	recHead := testkit.Request(t, app, httptest.NewRequest(http.MethodHead, "/api/v1/items/42", nil))
	if recHead.Code != http.StatusOK || recHead.Body.Len() != 0 {
		t.Fatalf("HEAD /api/v1/items/42 status=%d len=%d", recHead.Code, recHead.Body.Len())
	}

	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	_ = mw.WriteField("title", "pic")
	fw, err := mw.CreateFormFile("file", "pic.png")
	if err != nil {
		t.Fatalf("CreateFormFile failed: %v", err)
	}
	_, _ = fw.Write([]byte("PNG"))
	_ = mw.Close()

	upReq := httptest.NewRequest(http.MethodPost, "/api/v1/media", &mpBuf)
	upReq.Header.Set("Content-Type", mw.FormDataContentType())
	recUp := testkit.Request(t, app, upReq)
	if recUp.Code != http.StatusCreated {
		t.Fatalf("POST /api/v1/media status=%d body=%s", recUp.Code, recUp.Body.String())
	}
	assertVisitJSON(t, responseSchema(t, jsonDoc, "/api/v1/media", http.MethodPost, "201"), recUp.Body.Bytes())

	recFile := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/media/1", nil))
	if recFile.Code != http.StatusOK || recFile.Header().Get("Content-Type") != "image/png" || recFile.Body.String() != "PNG" {
		t.Fatalf("GET /media/1 status=%d ct=%q body=%q", recFile.Code, recFile.Header().Get("Content-Type"), recFile.Body.String())
	}

	recRSS := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/rss.xml", nil))
	if recRSS.Code != http.StatusOK || recRSS.Header().Get("Content-Type") != "application/rss+xml" || recRSS.Body.String() != "<rss/>" {
		t.Fatalf("GET /rss.xml status=%d ct=%q body=%q", recRSS.Code, recRSS.Header().Get("Content-Type"), recRSS.Body.String())
	}
}

func TestVerifyTypedStreamAndSSEOpenAPIParity(t *testing.T) {
	type sseReq struct {
		Topic string `query:"topic" default:"all"`
	}
	type binStreamReq struct {
		pi.Meta `summary:"Binary stream"`
	}

	app := newVerifyApp(t)
	app.GET("/events", func(ctx *pi.Context, req *sseReq) (*response.SSE, error) {
		return response.NewSSE(func(_ context.Context, sse *response.SSEWriter) error {
			return sse.Send(response.SSEEvent{ID: "1", Event: "topic", Data: req.Topic})
		}, response.WithSSEKeepAlive(0)), nil
	})
	app.HEAD("/events", func(ctx *pi.Context, req *sseReq) (*response.SSE, error) {
		return response.NewSSE(func(_ context.Context, sse *response.SSEWriter) error {
			return sse.SendData("never-sent")
		}, response.WithSSEKeepAlive(0)), nil
	})
	app.GET("/stream", func(ctx *pi.Context, _ *binStreamReq) (*response.Stream, error) {
		return &response.Stream{
			Run: func(_ context.Context, w io.Writer) error {
				_, err := io.WriteString(w, "raw-bytes")
				return err
			},
		}, nil
	})

	if err := app.RouteError(); err != nil {
		t.Fatalf("RouteError failed: %v", err)
	}

	jsonBytes, err := app.OpenAPIJSON()
	if err != nil {
		t.Fatalf("OpenAPIJSON failed: %v", err)
	}
	yamlBytes, err := app.OpenAPIYAML()
	if err != nil {
		t.Fatalf("OpenAPIYAML failed: %v", err)
	}

	jsonDoc := loadAndValidateDoc(t, jsonBytes)
	yamlDoc := loadAndValidateDoc(t, yamlBytes)

	for _, doc := range []*openapi3.T{jsonDoc, yamlDoc} {
		if doc.Components != nil && doc.Components.Schemas != nil {
			if _, leaked := doc.Components.Schemas["Stream"]; leaked {
				t.Fatalf("response.Stream must not appear in components.schemas")
			}
			if _, leaked := doc.Components.Schemas["SSE"]; leaked {
				t.Fatalf("response.SSE must not appear in components.schemas")
			}
		}

		eventsOp := operationFor(t, doc, "/events", http.MethodGet)
		resp200 := eventsOp.Responses.Value("200")
		if resp200 == nil || resp200.Value == nil {
			t.Fatalf("missing 200 response on GET /events")
		}
		mtSSE := resp200.Value.Content.Get("text/event-stream")
		if mtSSE == nil || mtSSE.Schema == nil || mtSSE.Schema.Value == nil {
			t.Fatalf("missing text/event-stream schema on GET /events")
		}
		if !mtSSE.Schema.Value.Type.Is("string") || mtSSE.Schema.Value.Format != "" {
			t.Fatalf("expected type=string format=\"\" for text/event-stream, got type=%v format=%q", mtSSE.Schema.Value.Type, mtSSE.Schema.Value.Format)
		}
		_ = operationFor(t, doc, "/events", http.MethodHead)

		streamOp := operationFor(t, doc, "/stream", http.MethodGet)
		stream200 := streamOp.Responses.Value("200")
		if stream200 == nil || stream200.Value == nil {
			t.Fatalf("missing 200 response on GET /stream")
		}
		mtBin := stream200.Value.Content.Get("application/octet-stream")
		if mtBin == nil || mtBin.Schema == nil || mtBin.Schema.Value == nil {
			t.Fatalf("missing application/octet-stream schema on GET /stream")
		}
		if !mtBin.Schema.Value.Type.Is("string") || mtBin.Schema.Value.Format != "binary" {
			t.Fatalf("expected type=string format=binary for /stream, got type=%v format=%q", mtBin.Schema.Value.Type, mtBin.Schema.Value.Format)
		}
	}

	recSSE := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/events?topic=orders", nil))
	if recSSE.Code != http.StatusOK ||
		recSSE.Header().Get("Content-Type") != "text/event-stream; charset=utf-8" ||
		recSSE.Header().Get("Cache-Control") != "no-cache, no-transform" ||
		recSSE.Header().Get("Connection") != "" ||
		recSSE.Body.String() != "id: 1\nevent: topic\ndata: orders\n\n" {
		t.Fatalf("GET /events unexpected response: status=%d hdr=%v body=%q", recSSE.Code, recSSE.Header(), recSSE.Body.String())
	}

	recHead := testkit.Request(t, app, httptest.NewRequest(http.MethodHead, "/events", nil))
	if recHead.Code != http.StatusOK ||
		recHead.Header().Get("Content-Type") != "text/event-stream; charset=utf-8" ||
		recHead.Body.Len() != 0 {
		t.Fatalf("HEAD /events unexpected response: status=%d ct=%q len=%d", recHead.Code, recHead.Header().Get("Content-Type"), recHead.Body.Len())
	}

	recBin := testkit.Request(t, app, httptest.NewRequest(http.MethodGet, "/stream", nil))
	if recBin.Code != http.StatusOK ||
		recBin.Header().Get("Content-Type") != "application/octet-stream" ||
		recBin.Body.String() != "raw-bytes" {
		t.Fatalf("GET /stream unexpected response: status=%d ct=%q body=%q", recBin.Code, recBin.Header().Get("Content-Type"), recBin.Body.String())
	}

	t.Run("rejects_incompatible_explicit_produces_on_sse", func(t *testing.T) {
		type badSSEReq struct {
			pi.Meta `produces:"application/json"`
		}
		badApp := newVerifyApp(t)
		badApp.GET("/bad-sse", func(_ *pi.Context, _ *badSSEReq) (*response.SSE, error) {
			return response.NewSSE(func(_ context.Context, _ *response.SSEWriter) error { return nil }), nil
		})
		if err := badApp.RouteError(); err == nil {
			t.Fatalf("expected RouteError for *response.SSE with produces:\"application/json\"")
		}
	})
}
