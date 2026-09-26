package migrations

import (
	"context"

	"github.com/sllt/pi/pkg/pi/migration"
)

func addEmployeeInRedis() migration.Migrate {
	return migration.Migrate{
		UP: func(d migration.Datasource) error {
			err := d.Redis.Set(context.Background(), "Umang", "0987654321", 0).Err()
			if err != nil {
				return err
			}

			return nil

		},
	}
}
