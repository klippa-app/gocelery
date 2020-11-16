// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisCeleryBackend is celery backend for redis
type RedisCeleryBackend struct {
	*redis.Client
}

// NewRedisBackend creates new RedisCeleryBackend with given redis pool.
// RedisCeleryBackend can be initialized manually as well.
func NewRedisBackend(conn *redis.Client) *RedisCeleryBackend {
	return &RedisCeleryBackend{
		Client: conn,
	}
}

// GetResult queries redis backend to get asynchronous result
func (cb *RedisCeleryBackend) GetResult(taskID string) (*ResultMessage, error) {
	val, err := cb.Client.Get(cb.Context(), fmt.Sprintf("celery-task-meta-%s", taskID)).Bytes()
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, fmt.Errorf("result not available")
	}
	var resultMessage ResultMessage
	err = json.Unmarshal(val, &resultMessage)
	if err != nil {
		return nil, err
	}
	return &resultMessage, nil
}

// SetResult pushes result back into redis backend
func (cb *RedisCeleryBackend) SetResult(taskID string, result *ResultMessage) error {
	resBytes, err := json.Marshal(result)
	if err != nil {
		return err
	}
	_, err = cb.Client.SetEX(cb.Context(), fmt.Sprintf("celery-task-meta-%s", taskID), resBytes, time.Second * 86400).Result()
	return err
}

// DeleteResult deletes the result from the redis backend
func (cb *RedisCeleryBackend) DeleteResult(taskID string) error {
	_, err := cb.Client.Del(cb.Context(), fmt.Sprintf("celery-task-meta-%s", taskID)).Result()
	return err
}
