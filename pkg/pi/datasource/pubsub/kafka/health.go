package kafka

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/sllt/pi/pkg/pi/datasource"
	"github.com/sllt/pi/pkg/pi/datasource/pubsub"
)

func (k *kafkaClient) Health() datasource.Health {
	health := datasource.Health{
		Status:  datasource.StatusDown,
		Details: make(map[string]any),
	}

	if k == nil {
		health.Details["error"] = "invalid connection type"
		return health
	}

	var (
		conn    *multiConn
		writer  Writer
		readers []Reader
		logger  pubsub.Logger
	)
	if k.mu != nil {
		k.mu.RLock()
		conn = k.conn
		writer = k.writer
		logger = k.logger
		if len(k.reader) > 0 {
			readers = make([]Reader, 0, len(k.reader))
			for _, r := range k.reader {
				if r != nil {
					readers = append(readers, r)
				}
			}
		}
		k.mu.RUnlock()
	} else {
		conn = k.conn
		writer = k.writer
		logger = k.logger
		for _, r := range k.reader {
			if r != nil {
				readers = append(readers, r)
			}
		}
	}

	if conn == nil {
		health.Details["error"] = "invalid connection type"
		return health
	}

	conn.mu.RLock()
	conns := append([]Connection(nil), conn.conns...)
	conn.mu.RUnlock()

	brokerStatus, allDown := evaluateBrokersHealth(conns)

	if !allDown {
		health.Status = datasource.StatusUp
	}

	health.Details["brokers"] = brokerStatus
	health.Details["backend"] = "KAFKA"
	health.Details["writer"] = writerStatsAsMap(writer, logger)
	health.Details["readers"] = readerStatsAsMap(readers, logger)

	return health
}

func (k *kafkaClient) evaluateBrokerHealth() ([]map[string]any, bool) {
	if k == nil || k.conn == nil {
		return make([]map[string]any, 0), true
	}
	k.conn.mu.RLock()
	conns := append([]Connection(nil), k.conn.conns...)
	k.conn.mu.RUnlock()
	return evaluateBrokersHealth(conns)
}

func evaluateBrokersHealth(conns []Connection) ([]map[string]any, bool) {
	var (
		statusList     = make([]map[string]any, 0, len(conns))
		controllerAddr string
		allDown        = true
	)

	for _, conn := range conns {
		if conn == nil {
			continue
		}

		status := checkBroker(conn, &controllerAddr)

		if status["status"] == brokerStatusUp {
			allDown = false
		}

		statusList = append(statusList, status)
	}

	return statusList, allDown
}

func checkBroker(conn Connection, controllerAddr *string) map[string]any {
	brokerAddr := conn.RemoteAddr().String()
	status := map[string]any{
		"broker":       brokerAddr,
		"status":       "DOWN",
		"isController": false,
		"error":        nil,
	}

	_, err := conn.ReadPartitions()
	if err != nil {
		status["error"] = err.Error()
		return status
	}

	status["status"] = brokerStatusUp

	if *controllerAddr == "" {
		controller, err := conn.Controller()
		if err != nil {
			status["error"] = fmt.Sprintf("controller lookup failed: %v", err)
		} else if controller.Host != "" {
			*controllerAddr = net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
		}
	}

	status["isController"] = brokerAddr == *controllerAddr

	return status
}

func (k *kafkaClient) getReaderStatsAsMap() []any {
	if k == nil {
		return make([]any, 0)
	}
	var (
		readers []Reader
		logger  pubsub.Logger
	)
	if k.mu != nil {
		k.mu.RLock()
		logger = k.logger
		for _, r := range k.reader {
			if r != nil {
				readers = append(readers, r)
			}
		}
		k.mu.RUnlock()
	} else {
		logger = k.logger
		for _, r := range k.reader {
			if r != nil {
				readers = append(readers, r)
			}
		}
	}
	return readerStatsAsMap(readers, logger)
}

func readerStatsAsMap(readers []Reader, logger pubsub.Logger) []any {
	readerStats := make([]any, 0, len(readers))

	for _, reader := range readers {
		if reader == nil {
			continue
		}
		var readerStat map[string]any
		if err := convertStructToMap(reader.Stats(), &readerStat); err != nil {
			if logger != nil {
				logger.Errorf("kafka Reader Stats processing failed: %v", err)
			}
			continue // Log the error but continue processing other readers
		}

		readerStats = append(readerStats, readerStat)
	}

	return readerStats
}

func (k *kafkaClient) getWriterStatsAsMap() map[string]any {
	if k == nil {
		return nil
	}
	var (
		writer Writer
		logger pubsub.Logger
	)
	if k.mu != nil {
		k.mu.RLock()
		writer = k.writer
		logger = k.logger
		k.mu.RUnlock()
	} else {
		writer = k.writer
		logger = k.logger
	}
	return writerStatsAsMap(writer, logger)
}

func writerStatsAsMap(writer Writer, logger pubsub.Logger) map[string]any {
	if writer == nil {
		return nil
	}
	writerStats := make(map[string]any)

	if err := convertStructToMap(writer.Stats(), &writerStats); err != nil {
		if logger != nil {
			logger.Errorf("kafka Writer Stats processing failed: %v", err)
		}

		return nil
	}

	return writerStats
}

// convertStructToMap tries to convert any struct to a map representation by first marshaling it to JSON, then unmarshalling into a map.
func convertStructToMap(input, output any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, &output)
	if err != nil {
		return err
	}

	return nil
}
