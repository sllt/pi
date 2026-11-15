package google

import (
	"context"
	"errors"
	"time"

	"google.golang.org/api/iterator"

	"github.com/sllt/pi/pkg/pi/datasource"
)

func (g *googleClient) Health() (health datasource.Health) {
	health.Details = make(map[string]any)
	health.Details["backend"] = "GOOGLE"

	if g == nil {
		health.Status = datasource.StatusDown
		return health
	}

	health.Details["projectID"] = g.Config.ProjectID

	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()

	if client == nil {
		health.Status = datasource.StatusDown
		health.Details["error"] = errClientNotConnected.Error()
		return health
	}

	var writerStatus, readerStatus string

	health.Status = datasource.StatusUp

	writerStatus, health.Details["writers"] = g.getWriterDetails()
	readerStatus, health.Details["readers"] = g.getReaderDetails()

	if readerStatus == datasource.StatusDown || writerStatus == datasource.StatusDown {
		health.Status = datasource.StatusDown
	}

	return health
}

//nolint:dupl // getWriterDetails provides the publishing details for current google publishers.
func (g *googleClient) getWriterDetails() (status string, details map[string]any) {
	const contextTimeoutDuration = 50

	details = make(map[string]any)
	if g == nil {
		return datasource.StatusDown, details
	}
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()
	if client == nil {
		return datasource.StatusDown, details
	}

	status = datasource.StatusUp

	ctx, cancel := context.WithTimeout(context.Background(), contextTimeoutDuration*time.Millisecond)
	defer cancel()

	it := client.Topics(ctx)

	for {
		topic, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			status = datasource.StatusDown

			break
		}

		if topic != nil {
			details[topic.ID()] = topic
		}
	}

	return status, details
}

//nolint:dupl // getReaderDetails provides the subscription details for current google subscriptions.
func (g *googleClient) getReaderDetails() (status string, details map[string]any) {
	const contextTimeoutDuration = 50

	details = make(map[string]any)
	if g == nil {
		return datasource.StatusDown, details
	}
	g.mu.RLock()
	client := g.client
	g.mu.RUnlock()
	if client == nil {
		return datasource.StatusDown, details
	}

	status = datasource.StatusUp

	ctx, cancel := context.WithTimeout(context.Background(), contextTimeoutDuration*time.Millisecond)
	defer cancel()

	subIt := client.Subscriptions(ctx)

	for {
		subscription, err := subIt.Next()
		if errors.Is(err, iterator.Done) {
			break
		}

		if err != nil {
			status = datasource.StatusDown

			break
		}

		if subscription != nil {
			details[subscription.ID()] = subscription
		}
	}

	return status, details
}
