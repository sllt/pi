package sqs

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/sllt/pi/pkg/pi/datasource"
)

const healthCheckTimeout = 5 * time.Second

// HealthCheckContext performs a context-aware health check of the SQS connection.
func (c *Client) HealthCheckContext(ctx context.Context) datasource.Health {
	if c == nil {
		return datasource.Health{Status: datasource.StatusDown}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	var region string
	if c.cfg != nil {
		region = c.cfg.Region
	}

	health := datasource.Health{
		Details: map[string]any{
			"backend": "SQS",
			"region":  region,
		},
	}

	conn := c.getConn()
	if conn == nil {
		health.Status = datasource.StatusDown
		health.Details["error"] = "client not connected"

		return health
	}

	if err := ctx.Err(); err != nil {
		health.Status = datasource.StatusDown
		health.Details["error"] = err.Error()
		return health
	}

	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, healthCheckTimeout)
		defer cancel()
	}

	// Use ListQueues with MaxResults=1 as a lightweight health check
	_, err := conn.ListQueues(ctx, &sqs.ListQueuesInput{
		MaxResults: aws.Int32(1),
	})
	if err != nil {
		health.Status = datasource.StatusDown
		health.Details["error"] = err.Error()

		return health
	}

	health.Status = datasource.StatusUp

	return health
}

// Health returns the health status of the SQS connection.
func (c *Client) Health() datasource.Health {
	ctx, cancel := context.WithTimeout(context.Background(), healthCheckTimeout)
	defer cancel()
	return c.HealthCheckContext(ctx)
}
