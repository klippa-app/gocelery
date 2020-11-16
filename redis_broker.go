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

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Client
	queueName string
}

// NewRedisBroker creates new RedisCeleryBroker with given redis connection pool
func NewRedisBroker(conn *redis.Client) *RedisCeleryBroker {
	return &RedisCeleryBroker{
		Client:      conn,
		queueName: "celery",
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	_, err = cb.Client.LPush(cb.Context(), cb.queueName, jsonBytes).Result()
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (*CeleryMessage, error) {
	messageList, err := cb.Client.BRPop(cb.Context(), time.Second * 1, cb.queueName).Result()
	if err != nil {
		return nil, err
	}

	if messageList == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	if messageList[0] != "celery" {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	var message CeleryMessage
	if err := json.Unmarshal([]byte(messageList[1]), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}
