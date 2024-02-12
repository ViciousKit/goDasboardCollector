package cache

import (
	"sync"

	"github.com/bradfitz/gomemcache/memcache"
)

type Cache interface {
	Get(key string) string
	Set(key string, value string)
}

type cache struct {
	client memcache.Client
}

var cacheInstance *cache

var once sync.Once

func (c *cache) Get(key string) string {
	return "default value"
}

func (c *cache) Set(key string, value string) {
}

func GetCache(server string) Cache {
	once.Do(func() {
		client := memcache.New(server)
		cacheInstance = &cache{}
	})

	return cacheInstance
}

func SetGauge() {
}
