// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package main

import (
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/klippa-app/gocelery"
)

// Run Celery Worker First!
// celery -A worker worker --loglevel=debug --without-heartbeat --without-mingle
func main() {
	// create redis connection client
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// initialize celery client
	cli, _ := gocelery.NewCeleryClient(
		gocelery.NewRedisBroker(redisClient),
		&gocelery.RedisCeleryBackend{Client: redisClient},
		1,
	)

	// prepare arguments
	taskName := "worker.add"
	argA := rand.Intn(10)
	argB := rand.Intn(10)

	// run task
	task := gocelery.GetTaskMessage(taskName)
	task.Args = append(task.Args, argA, argB)
	asyncResult, err := cli.Delay(task)
	if err != nil {
		panic(err)
	}

	// get results from backend with timeout
	res, err := asyncResult.Get(10 * time.Second)
	if err != nil {
		panic(err)
	}

	log.Printf("result: %+v of type %+v", res, reflect.TypeOf(res))

}
