package create

// Handler template for Pi framework
const handlerTemplate = `package handler

import (
	"{{ .ProjectName }}/internal/service"
	"github.com/sllt/pi/pkg/pi"
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

// Get{{ .StructName }} godoc
// @Summary Get {{ .StructName }}
// @Description Get {{ .StructName }} by ID
// @Tags {{ .StructName }}
// @Accept json
// @Produce json
// @Param id path string true "{{ .StructName }} ID"
// @Success 200 {object} any
// @Router /{{ .StructNameSnakeCase }}/:id [get]
func (h *{{ .StructName }}Handler) Get{{ .StructName }}(ctx *pi.Context) (any, error) {
	id := ctx.PathParam("id")
	return h.{{ .StructNameLowerFirst }}Service.Get{{ .StructName }}(ctx, id)
}

// Create{{ .StructName }} godoc
// @Summary Create {{ .StructName }}
// @Description Create a new {{ .StructName }}
// @Tags {{ .StructName }}
// @Accept json
// @Produce json
// @Success 200 {object} any
// @Router /{{ .StructNameSnakeCase }} [post]
func (h *{{ .StructName }}Handler) Create{{ .StructName }}(ctx *pi.Context) (any, error) {
	// TODO: implement create logic
	return nil, nil
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
	case "handler":
		return handlerTemplate
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
