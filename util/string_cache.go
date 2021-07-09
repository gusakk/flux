// Copyright © 2019 Dell Inc. or its subsidiaries.
// All Rights Reserved.
// This software contains the intellectual property of Dell Inc. or is licensed to Dell Inc.
// from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the
// License Agreement under which it is provided by or on behalf of Dell Inc. or its subsidiaries.

package intern

import (
	"container/list"
	"github.com/prometheus/client_golang/prometheus"
	"sync"
)

// Cache provides the function to manipulate the cached data.
type Cache interface {
	// Add adds the data to the cache and return the added element
	// or just return the data if it has been already added to the cache before.
	Add(data string) string
	// Get gets the data from the cache or return false if the data does not exist in the cache.
	Get(data string) (string, bool)
	// PrometheusCollectors return prometheus metrics collectors to register cache metrics.
	PrometheusCollectors() []prometheus.Collector
}

type StringCache struct {
	items        map[string]*list.Element
	evictionList *list.List
	maxCacheSize int64
	metrics      *stringCacheMetrics

	lock sync.RWMutex
}

// NewStringCache produces new string cache instance.
func NewStringCache(maxCacheSize int64) *StringCache {
	cache := &StringCache{
		items: make(map[string]*list.Element),
		// max cache size in bytes.
		maxCacheSize: maxCacheSize,
		evictionList: list.New(),
		metrics:      newStringCacheMetrics(),
	}
	return cache
}

func (c *StringCache) Add(data string) string {
	c.lock.Lock()
	defer c.lock.Unlock()

	if element, ok := c.items[data]; ok {
		c.evictionList.MoveToFront(element)
		return element.Value.(string)
	}

	c.items[data] = c.evictionList.PushFront(data)
	c.metrics.itemsCount.WithLabelValues().Inc()

	c.evictOldest()

	return data
}

func (c *StringCache) Get(data string) (string, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if element := c.items[data]; element != nil {
		return element.Value.(string), true
	}

	return "", false
}

func (c *StringCache) PrometheusCollectors() []prometheus.Collector {
	return c.metrics.PrometheusCollectors()
}

func (c *StringCache) evictOldest() {
	for c.evictionRequired() {
		element := c.evictionList.Back()
		if element == nil {
			return
		}
		c.removeElement(element)
	}
}

func (c *StringCache) evictionRequired() bool {
	return int64(len(c.items)) > c.maxCacheSize
}

func (c *StringCache) removeElement(element *list.Element) {
	item := c.evictionList.Remove(element).(string)
	delete(c.items, item)
	c.metrics.itemsCount.WithLabelValues().Dec()
}

// NoopStringCache just returns the incoming strings.
type NoopStringCache struct{}

func (c *NoopStringCache) Add(data string) string {
	return data
}

func (c *NoopStringCache) Get(data string) (string, bool) {
	return data, true
}

func (c *NoopStringCache) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{}
}
