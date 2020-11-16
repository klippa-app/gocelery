// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"time"

	"github.com/go-redis/redis/v8"
)

func Example_worker() {
	// create redis connection client
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// initialize celery client
	cli, _ := NewCeleryClient(
		NewRedisBroker(redisClient),
		&RedisCeleryBackend{Client: redisClient},
		5, // number of workers
	)

	// task
	add := func(a, b int) int {
		return a + b
	}

	// register task
	cli.Register("add", add)

	// start workers (non-blocking call)
	cli.StartWorker()

	// wait for client request
	time.Sleep(10 * time.Second)

	// stop workers gracefully (blocking call)
	cli.StopWorker()

}
