package create

// DTO template for Pi typed HTTP contracts
const dtoTemplate = `package v1

import (
	"time"

	"github.com/sllt/pi/pkg/pi"
)

type Get{{ .StructName }}Req struct {
	pi.Meta ` + "`path:\"/{id}\" method:\"GET\" tags:\"{{ .StructName }}\" summary:\"Get {{ .StructName }}\"`" + `

	ID string ` + "`path:\"id\" validate:\"required\" dc:\"{{ .StructName }} ID\"`" + `
}

type Get{{ .StructName }}Res struct {
	ID        uint      ` + "`json:\"id\"`" + `
	CreatedAt time.Time ` + "`json:\"createdAt\"`" + `
	UpdatedAt time.Time ` + "`json:\"updatedAt\"`" + `
}

type Create{{ .StructName }}Req struct {
	pi.Meta ` + "`path:\"\" method:\"POST\" status:\"201\" tags:\"{{ .StructName }}\" summary:\"Create {{ .StructName }}\"`" + `
}

type Create{{ .StructName }}Res struct {
	ID uint ` + "`json:\"id\"`" + `
}
`

// Handler template for Pi framework
const handlerTemplate = `package handler

import (
	"net/http"

	v1 "{{ .ProjectName }}/api/v1"
	"{{ .ProjectName }}/internal/model"
	"{{ .ProjectName }}/internal/service"
	"github.com/sllt/pi/pkg/pi"
	"github.com/sllt/pi/pkg/pi/apperror"
)

type {{ .StructName }}Handler struct {
	*Handler
	{{ .StructNameLowerFirst }}Service service.{{ .StructName }}Service
}

func New{{ .StructName }}Handler(
	handler *Handler,
	{{ .StructNameLowerFirst }}Service service.{{ .StructName }}Service,
) *{{ .StructName }}Handler {
	return &{{ .StructName }}Handler{
		Handler:                 handler,
		{{ .StructNameLowerFirst }}Service: {{ .StructNameLowerFirst }}Service,
	}
}

func (h *{{ .StructName }}Handler) Get{{ .StructName }}(ctx *pi.Context, req *v1.Get{{ .StructName }}Req) (*v1.Get{{ .StructName }}Res, error) {
	item, err := h.{{ .StructNameLowerFirst }}Service.Get{{ .StructName }}(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, apperror.New(apperror.NotFound, http.StatusNotFound, "{{ .StructNameLowerFirst }} not found")
	}
	return &v1.Get{{ .StructName }}Res{
		ID:        item.Id,
		CreatedAt: item.CreatedAt,
		UpdatedAt: item.UpdatedAt,
	}, nil
}

func (h *{{ .StructName }}Handler) Create{{ .StructName }}(ctx *pi.Context, _ *v1.Create{{ .StructName }}Req) (*v1.Create{{ .StructName }}Res, error) {
	item := &model.{{ .StructName }}{}
	if err := h.{{ .StructNameLowerFirst }}Service.Create{{ .StructName }}(ctx, item); err != nil {
		return nil, err
	}
	return &v1.Create{{ .StructName }}Res{
		ID: item.Id,
	}, nil
}
`

// Router template for Pi framework
const routerTemplate = `package router

import (
	"{{ .ProjectName }}/internal/handler"
	"github.com/sllt/pi/pkg/pi"
)

func Init{{ .StructName }}Router(deps RouterDeps, h *handler.{{ .StructName }}Handler) {
	group := deps.App.Group("/api/v1").Group("/{{ .StructNameSnakeCase }}")
	_ = pi.Register(group, h.Get{{ .StructName }})
	_ = pi.Register(group, h.Create{{ .StructName }})
}
`

// Service template for Pi framework
const serviceTemplate = `package service

import (
	"context"

	"{{ .ProjectName }}/internal/model"
	"{{ .ProjectName }}/internal/repository"
)

type {{ .StructName }}Service interface {
	Get{{ .StructName }}(ctx context.Context, id string) (*model.{{ .StructName }}, error)
	Create{{ .StructName }}(ctx context.Context, {{ .StructNameLowerFirst }} *model.{{ .StructName }}) error
}

func New{{ .StructName }}Service(
	service *Service,
	{{ .StructNameLowerFirst }}Repository repository.{{ .StructName }}Repository,
) {{ .StructName }}Service {
	return &{{ .StructNameLowerFirst }}Service{
		Service:                    service,
		{{ .StructNameLowerFirst }}Repository: {{ .StructNameLowerFirst }}Repository,
	}
}

type {{ .StructNameLowerFirst }}Service struct {
	*Service
	{{ .StructNameLowerFirst }}Repository repository.{{ .StructName }}Repository
}

func (s *{{ .StructNameLowerFirst }}Service) Get{{ .StructName }}(ctx context.Context, id string) (*model.{{ .StructName }}, error) {
	return s.{{ .StructNameLowerFirst }}Repository.Get{{ .StructName }}(ctx, id)
}

func (s *{{ .StructNameLowerFirst }}Service) Create{{ .StructName }}(ctx context.Context, {{ .StructNameLowerFirst }} *model.{{ .StructName }}) error {
	return s.{{ .StructNameLowerFirst }}Repository.Create{{ .StructName }}(ctx, {{ .StructNameLowerFirst }})
}
`

// Repository template for Pi framework
const repositoryTemplate = `package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"{{ .ProjectName }}/internal/model"
)

type {{ .StructName }}Repository interface {
	Get{{ .StructName }}(ctx context.Context, id string) (*model.{{ .StructName }}, error)
	Create{{ .StructName }}(ctx context.Context, {{ .StructNameLowerFirst }} *model.{{ .StructName }}) error
}

func New{{ .StructName }}Repository(
	r *Repository,
) {{ .StructName }}Repository {
	return &{{ .StructNameLowerFirst }}Repository{
		Repository: r,
	}
}

type {{ .StructNameLowerFirst }}Repository struct {
	*Repository
}

func (r *{{ .StructNameLowerFirst }}Repository) Get{{ .StructName }}(ctx context.Context, id string) (*model.{{ .StructName }}, error) {
	var {{ .StructNameLowerFirst }} model.{{ .StructName }}
	q := r.GetQuerier(ctx)
	err := q.Select(ctx, &{{ .StructNameLowerFirst }},
		"SELECT * FROM {{ .StructNameSnakeCase }}s WHERE id = ?",
		id,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &{{ .StructNameLowerFirst }}, nil
}

func (r *{{ .StructNameLowerFirst }}Repository) Create{{ .StructName }}(ctx context.Context, {{ .StructNameLowerFirst }} *model.{{ .StructName }}) error {
	now := time.Now()
	{{ .StructNameLowerFirst }}.CreatedAt = now
	{{ .StructNameLowerFirst }}.UpdatedAt = now

	q := r.GetQuerier(ctx)
	result, err := q.ExecContext(ctx,
		"INSERT INTO {{ .StructNameSnakeCase }}s (created_at, updated_at) VALUES (?, ?)",
		{{ .StructNameLowerFirst }}.CreatedAt, {{ .StructNameLowerFirst }}.UpdatedAt,
	)
	if err != nil {
		return err
	}

	id, err := result.LastInsertId()
	if err == nil {
		{{ .StructNameLowerFirst }}.Id = uint(id)
	}

	return nil
}
`

// Model template for Pi framework
const modelTemplate = `package model

import "time"

type {{ .StructName }} struct {
	Id        uint      ` + "`db:\"id\"`" + `
	CreatedAt time.Time ` + "`db:\"created_at\"`" + `
	UpdatedAt time.Time ` + "`db:\"updated_at\"`" + `
}

func (m *{{ .StructName }}) TableName() string {
	return "{{ .StructNameSnakeCase }}s"
}
`

// GetTemplate returns the template content for the given type.
func GetTemplate(createType string) string {
	switch createType {
	case "dto":
		return dtoTemplate
	case "handler":
		return handlerTemplate
	case "router":
		return routerTemplate
	case "service":
		return serviceTemplate
	case "repository":
		return repositoryTemplate
	case "model":
		return modelTemplate
	default:
		return ""
	}
}
