package main

import (
	"cache"
	"counters"
)

type Worker struct {
	name   string
	method func()
}

var workers = []Worker{
	{
		"socialNetworkCounters",
		counters.SocialCountersWorker,
	},
}

func main() {
	cache.GetCache("localhost:11211")
	// for _, worker := range workers {
	// 	worker.method()
	// }
}
