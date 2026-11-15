package migration

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/sllt/pi/pkg/pi/logging"
)

var (
	errTopic = errors.New("topic error")
	errQuery = errors.New("query error")
)

func pubsubTestSetup(t *testing.T) (migrator, *fakePubSub, *Deps) {
	t.Helper()

	mockPubSub := &fakePubSub{}
	deps := &Deps{
		Logger: logging.NewWriterLogger(logging.DEBUG, io.Discard, io.Discard),
		PubSub: mockPubSub,
	}

	ds := Datasource{PubSub: mockPubSub}
	pubsubDB := pubsubDS{client: mockPubSub}
	migratorWithPubSub := pubsubDB.apply(&ds)

	return migratorWithPubSub, mockPubSub, deps
}

func Test_CreateTopic(t *testing.T) {
	testCases := []struct {
		desc     string
		topic    string
		mockErr  error
		expected error
	}{
		{"successfully create topic", "test-topic", nil, nil},
		{"error creating topic", "test-topic", errTopic, errTopic},
	}

	for _, tc := range testCases {
		mockPubSub := &fakePubSub{
			createTopicFn: func(_ context.Context, name string) error {
				assert.Equal(t, tc.topic, name)
				return tc.mockErr
			},
		}
		ds := pubsubDS{client: mockPubSub}

		err := ds.CreateTopic(t.Context(), tc.topic)

		assert.Equal(t, tc.expected, err, tc.desc)
	}
}

func Test_DeleteTopic(t *testing.T) {
	testCases := []struct {
		desc     string
		topic    string
		mockErr  error
		expected error
	}{
		{"successfully delete topic", "test-topic", nil, nil},
		{"error deleting topic", "test-topic", errTopic, errTopic},
	}

	for _, tc := range testCases {
		mockPubSub := &fakePubSub{
			deleteTopicFn: func(_ context.Context, name string) error {
				assert.Equal(t, tc.topic, name)
				return tc.mockErr
			},
		}
		ds := pubsubDS{client: mockPubSub}

		err := ds.DeleteTopic(t.Context(), tc.topic)

		assert.Equal(t, tc.expected, err, tc.desc)
	}
}

func Test_Query(t *testing.T) {
	testCases := []struct {
		desc     string
		query    string
		args     []any
		mockResp []byte
		mockErr  error
		expected []byte
		err      error
	}{
		{"successful query", "SELECT * FROM test", []any{1, 2}, []byte("result"), nil, []byte("result"), nil},
		{"error in query", "SELECT * FROM test", []any{1, 2}, nil, errQuery, nil, errQuery},
	}

	for _, tc := range testCases {
		mockPubSub := &fakePubSub{
			queryFn: func(_ context.Context, query string, args ...any) ([]byte, error) {
				assert.Equal(t, tc.query, query)
				assert.Equal(t, tc.args, args)
				return tc.mockResp, tc.mockErr
			},
		}
		ds := pubsubDS{client: mockPubSub}

		resp, err := ds.Query(t.Context(), tc.query, tc.args...)

		assert.Equal(t, tc.expected, resp, tc.desc)
		assert.Equal(t, tc.err, err, tc.desc)
	}
}

func Test_PubSubCheckAndCreateMigrationTable(t *testing.T) {
	migratorWithPubSub, mockPubSub, deps := pubsubTestSetup(t)

	testCases := []struct {
		desc      string
		createErr error
	}{
		{"no error", nil},
		{"topic already exists", errTopic},
	}

	for i, tc := range testCases {
		mockPubSub.createTopicFn = func(_ context.Context, name string) error {
			assert.Equal(t, pubsubMigrationTopic, name)
			return tc.createErr
		}

		err := migratorWithPubSub.checkAndCreateMigrationTable(t.Context(), deps)

		assert.NoError(t, err, "TEST[%v]\n %v Failed! ", i, tc.desc)
	}
}

func Test_PubSubCommitMigration_Success(t *testing.T) {
	migratorWithPubSub, mockPubSub, deps := pubsubTestSetup(t)

	fixedTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	data := transactionData{
		MigrationNumber: 123,
		StartTime:       fixedTime,
	}

	published := false
	mockPubSub.publishFn = func(_ context.Context, name string, _ []byte) error {
		assert.Equal(t, pubsubMigrationTopic, name)
		published = true
		return nil
	}

	err := migratorWithPubSub.commitMigration(t.Context(), deps, data)

	assert.NoError(t, err, "Successful migration commit should not return an error")
	assert.True(t, published)
}

func Test_PubSubCommitMigration_PublishError(t *testing.T) {
	migratorWithPubSub, mockPubSub, deps := pubsubTestSetup(t)

	fixedTime := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	data := transactionData{
		MigrationNumber: 123,
		StartTime:       fixedTime,
	}

	mockPubSub.publishFn = func(_ context.Context, _ string, _ []byte) error {
		return errTopic
	}

	err := migratorWithPubSub.commitMigration(t.Context(), deps, data)

	assert.Equal(t, errTopic, err, "Publish error should be returned")
}

func Test_PubSubGetLastMigration(t *testing.T) {
	testCases := []struct {
		desc           string
		expectedResult int64
		pubsubResult   []byte
		pubsubError    error
		nextVersion    int64
		nextErr        error
		expectedErr    error
	}{
		{
			desc:           "pubsub has higher version than next migrator",
			expectedResult: 3,
			nextVersion:    2,
			pubsubResult: []byte(`{"version":1,"method":"UP","start_time":1625000000000,"duration":100}
{"version":3,"method":"UP","start_time":1625000200000,"duration":150}
{"version":2,"method":"DOWN","start_time":1625000100000,"duration":120}`),
			pubsubError: nil,
		},
		{
			desc:           "next migrator has higher version than pubsub",
			expectedResult: 5,
			nextVersion:    5,
			pubsubResult: []byte(`{"version":1,"method":"UP","start_time":1625000000000,"duration":100}
{"version":3,"method":"UP","start_time":1625000200000,"duration":150}`),
			pubsubError: nil,
		},
		{
			desc:           "query error but next migrator has value",
			expectedResult: -1,
			nextVersion:    4,
			pubsubResult:   nil,
			pubsubError:    errQuery,
			expectedErr:    errQuery,
		},
		{
			desc:           "empty result but next migrator has value",
			expectedResult: 3,
			nextVersion:    3,
			pubsubResult:   []byte{},
			pubsubError:    nil,
		},
	}

	for i, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			mockPubSub := &fakePubSub{
				queryFn: func(_ context.Context, query string, args ...any) ([]byte, error) {
					assert.Equal(t, pubsubMigrationTopic, query)
					assert.Equal(t, []any{int64(0), defaultQueryLimit}, args)
					return tc.pubsubResult, tc.pubsubError
				},
			}
			deps := &Deps{
				Logger: logging.NewWriterLogger(logging.DEBUG, io.Discard, io.Discard),
				PubSub: mockPubSub,
			}

			next := &fakeMigrator{
				lastFn: func(context.Context, *Deps) (int64, error) {
					return tc.nextVersion, tc.nextErr
				},
			}

			pm := pubsubMigrator{
				PubSub:   pubsubDS{client: mockPubSub},
				migrator: next,
			}

			result, err := pm.getLastMigration(t.Context(), deps)

			assert.Equal(t, tc.expectedResult, result, "TEST[%v] %v Failed!", i, tc.desc)

			if tc.expectedErr != nil {
				assert.ErrorContains(t, err, tc.expectedErr.Error(), "TEST[%v] %v Failed!", i, tc.desc)
			} else {
				assert.NoError(t, err, "TEST[%v] %v Failed!", i, tc.desc)
			}
		})
	}
}
