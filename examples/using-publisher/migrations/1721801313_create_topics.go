package migrations

import (
	"context"

	"github.com/sllt/pi/pkg/pi/migration"
)

func createTopics() migration.Migrate {
	return migration.Migrate{
		UP: func(d migration.Datasource) error {
			err := d.PubSub.CreateTopic(context.Background(), "products")
			if err != nil {
				return err
			}

			return d.PubSub.CreateTopic(context.Background(), "order-logs")
		},
	}
}
