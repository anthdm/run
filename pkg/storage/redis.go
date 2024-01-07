package storage

import (
	"context"
	"fmt"

	"github.com/anthdm/raptor/pkg/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

// RedisStore is the Redis implementation of a Storage interface.
type RedisStore struct {
	client *redis.Client
}

// NewRedisStore returns a new RedisStore.
//
// This will return an error if we failed to ping the Redis server.
func NewRedisStore() (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{})
	err := client.Ping(context.Background()).Err()
	if err != nil {
		err = fmt.Errorf("failed to connect to the Redis server: %s", err)
		return nil, err
	}
	return &RedisStore{
		client: client,
	}, nil
}

func (s *RedisStore) CreateEndpoint(endpoint *types.Endpoint) error {
	b, err := msgpack.Marshal(endpoint)
	if err != nil {
		return err
	}
	key := makeKey("endpoint", endpoint.ID)
	return s.client.Set(context.Background(), key, b, 0).Err()
}

func (s *RedisStore) GetEndpoint(id uuid.UUID) (*types.Endpoint, error) {
	key := makeKey("endpoint", id)
	return s.getEndpoint(key)
}

func (s *RedisStore) getEndpoint(id string) (*types.Endpoint, error) {
	var endpoint types.Endpoint
	b, err := s.client.Get(context.Background(), id).Bytes()
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &endpoint)
	return &endpoint, err
}

func (s *RedisStore) GetEndpoints() ([]types.Endpoint, error) {
	var (
		cursor  uint64
		pattern = "endpoint_*"
	)
	keys, cursor, err := s.client.Scan(context.Background(), cursor, pattern, 0).Result()
	if err != nil {
		return []types.Endpoint{}, err
	}

	endpoints := make([]types.Endpoint, len(keys))
	for i, key := range keys {
		endpoint, err := s.getEndpoint(key)
		if err != nil {
			return nil, err
		}
		endpoints[i] = *endpoint
	}

	return endpoints, nil
}

func (s *RedisStore) UpdateEndpoint(id uuid.UUID, params UpdateEndpointParams) error {
	endpoint, err := s.GetEndpoint(id)
	if err != nil {
		return err
	}
	if params.ActiveDeployID.String() != "00000000-0000-0000-0000-000000000000" {
		endpoint.ActiveDeployID = params.ActiveDeployID
	}
	if params.Environment != nil {
		for key, val := range params.Environment {
			endpoint.Environment[key] = val
		}
	}
	if params.DeployHistory != nil {
		endpoint.DeployHistory = append(endpoint.DeployHistory, params.DeployHistory)
	}
	return s.CreateEndpoint(endpoint)
}

func (s *RedisStore) GetDeploy(id uuid.UUID) (*types.Deploy, error) {
	var deploy types.Deploy
	b, err := s.client.Get(context.Background(), id.String()).Bytes()
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &deploy)
	return &deploy, err
}

func (s *RedisStore) CreateDeploy(deploy *types.Deploy) error {
	b, err := msgpack.Marshal(deploy)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), deploy.ID.String(), b, 0).Err()
}

func (s *RedisStore) CreateRuntimeMetric(metric *types.RuntimeMetric) error {
	b, err := msgpack.Marshal(metric)
	if err != nil {
		return err
	}
	return s.client.Set(context.Background(), metric.ID.String(), b, 0).Err()
}

func (s *RedisStore) GetRuntimeMetrics(id uuid.UUID) ([]types.RuntimeMetric, error) {
	var metrics []types.RuntimeMetric
	b, err := s.client.Get(context.Background(), id.String()).Bytes()
	if err != nil {
		return nil, err
	}
	err = msgpack.Unmarshal(b, &metrics)
	return metrics, err
}

func makeKey(prefix string, id uuid.UUID) string {
	return fmt.Sprintf("%s_%s", prefix, id)
}
