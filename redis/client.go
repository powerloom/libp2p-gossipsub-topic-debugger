package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var RedisClient *redis.Client

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Host         string
	Port         string
	DB           int
	Password     string
	PoolSize     int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	DialTimeout  time.Duration
	IdleTimeout  time.Duration
}

// NewRedisClient creates a new Redis client from environment variables
func NewRedisClient() (*redis.Client, error) {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6379"
	}

	db := 0
	if dbStr := os.Getenv("REDIS_DB"); dbStr != "" {
		if parsedDB, err := strconv.Atoi(dbStr); err == nil {
			db = parsedDB
		}
	}

	password := os.Getenv("REDIS_PASSWORD")

	cfg := RedisConfig{
		Host:         host,
		Port:         port,
		DB:           db,
		Password:     password,
		PoolSize:     100,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		DialTimeout:  5 * time.Second,
		IdleTimeout:  5 * time.Minute,
	}

	client := redis.NewClient(&redis.Options{
		Addr:            fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password:        cfg.Password,
		DB:              cfg.DB,
		PoolSize:        cfg.PoolSize,
		DialTimeout:     cfg.DialTimeout,
		ConnMaxIdleTime: cfg.IdleTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return client, nil
}

// Get retrieves a value from Redis
func Get(ctx context.Context, key string) (string, error) {
	if RedisClient == nil {
		return "", errors.New("Redis client not initialized")
	}
	val, err := RedisClient.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

// Set sets a value in Redis
func Set(ctx context.Context, key, value string) error {
	if RedisClient == nil {
		return errors.New("Redis client not initialized")
	}
	return RedisClient.Set(ctx, key, value, 0).Err()
}

// SetWithExpiration sets a value in Redis with expiration
func SetWithExpiration(ctx context.Context, key, value string, expiration time.Duration) error {
	if RedisClient == nil {
		return errors.New("Redis client not initialized")
	}
	return RedisClient.Set(ctx, key, value, expiration).Err()
}

// Incr increments a value in Redis by 1 (returns new value)
func Incr(ctx context.Context, key string) (int64, error) {
	if RedisClient == nil {
		return 0, errors.New("Redis client not initialized")
	}
	return RedisClient.Incr(ctx, key).Result()
}

// IncrBy increments a value in Redis by the specified amount (returns new value)
func IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	if RedisClient == nil {
		return 0, errors.New("Redis client not initialized")
	}
	return RedisClient.IncrBy(ctx, key, value).Result()
}

// HSet sets a field in a Redis hash
func HSet(ctx context.Context, key, field string, value interface{}) error {
	if RedisClient == nil {
		return errors.New("Redis client not initialized")
	}
	return RedisClient.HSet(ctx, key, field, value).Err()
}

// HGet retrieves a field from a Redis hash
func HGet(ctx context.Context, key, field string) (string, error) {
	if RedisClient == nil {
		return "", errors.New("Redis client not initialized")
	}
	val, err := RedisClient.HGet(ctx, key, field).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return val, nil
}

// HGetAll retrieves all fields from a Redis hash
func HGetAll(ctx context.Context, key string) (map[string]string, error) {
	if RedisClient == nil {
		return nil, errors.New("Redis client not initialized")
	}
	return RedisClient.HGetAll(ctx, key).Result()
}

// SAdd adds members to a Redis set
func SAdd(ctx context.Context, key string, members ...interface{}) error {
	if RedisClient == nil {
		return errors.New("Redis client not initialized")
	}
	return RedisClient.SAdd(ctx, key, members...).Err()
}

// SMembers retrieves all members from a Redis set
func SMembers(ctx context.Context, key string) ([]string, error) {
	if RedisClient == nil {
		return nil, errors.New("Redis client not initialized")
	}
	return RedisClient.SMembers(ctx, key).Result()
}

// SCard returns the cardinality of a Redis set
func SCard(ctx context.Context, key string) (int64, error) {
	if RedisClient == nil {
		return 0, errors.New("Redis client not initialized")
	}
	return RedisClient.SCard(ctx, key).Result()
}

// SRem removes members from a Redis set
func SRem(ctx context.Context, key string, members ...interface{}) error {
	if RedisClient == nil {
		return errors.New("Redis client not initialized")
	}
	return RedisClient.SRem(ctx, key, members...).Err()
}

// Expire sets expiration on a Redis key
func Expire(ctx context.Context, key string, expiration time.Duration) error {
	if RedisClient == nil {
		return errors.New("Redis client not initialized")
	}
	return RedisClient.Expire(ctx, key, expiration).Err()
}

// Del deletes one or more Redis keys
func Del(ctx context.Context, keys ...string) (int64, error) {
	if RedisClient == nil {
		return 0, errors.New("Redis client not initialized")
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return RedisClient.Del(ctx, keys...).Result()
}

// Unlink removes one or more Redis keys asynchronously (non-blocking)
func Unlink(ctx context.Context, keys ...string) (int64, error) {
	if RedisClient == nil {
		return 0, errors.New("Redis client not initialized")
	}
	if len(keys) == 0 {
		return 0, nil
	}
	return RedisClient.Unlink(ctx, keys...).Result()
}
