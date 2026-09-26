package migrations

import (
	"github.com/sllt/pi/pkg/pi/migration"
)

func All() map[int64]migration.Migrate {
	return map[int64]migration.Migrate{
		1722507126: createTableEmployee(),
		1722507180: addEmployeeInRedis(),
	}
}
