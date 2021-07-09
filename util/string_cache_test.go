// Copyright © 2019 Dell Inc. or its subsidiaries.
// All Rights Reserved.
// This software contains the intellectual property of Dell Inc. or is licensed to Dell Inc.
// from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the
// License Agreement under which it is provided by or on behalf of Dell Inc. or its subsidiaries.

package intern

import (
	"github.com/google/go-cmp/cmp"
	"testing"
)

func TestSetNewItem(t *testing.T) {
	cache := NewStringCache(100000)
	key := "key"
	cache.Add(key)

	if _, ok := cache.items[key]; !ok {
		t.Errorf("element with key: %s should exist in the cache", key)
	}

	item := cache.evictionList.Front().Value.(string)
	expectedItem := key

	if !cmp.Equal(item, expectedItem) {
		t.Errorf("items should be equal, got: %v, want: %v", item, expectedItem)
	}
}

func TestSetExistingItem(t *testing.T) {
	cache := NewStringCache(100000)
	key := "key"
	// initial set
	cache.Add(key)
	cache.Add("newkey")
	// update of existing element
	cache.Add(key)

	if _, ok := cache.items[key]; !ok {
		t.Errorf("element with key: %s should exist in the cache", key)
	}

	item := cache.evictionList.Front().Value.(string)
	expectedItem := key

	if !cmp.Equal(item, expectedItem) {
		t.Errorf("items should be equal, got: %v, want: %v", item, expectedItem)
	}
}

func TestGetItemFromCache(t *testing.T) {
	cache := NewStringCache(100000)
	key := "key"
	// initial set
	cache.Add(key)
	cache.Add("key2")

	result, _ := cache.Get(key)
	if result != "key" {
		t.Errorf("results should be equal, got: %s, want: %s", result, "key")
	}

	item := cache.evictionList.Front().Value.(string)
	expectedItem := "key2"

	if !cmp.Equal(item, expectedItem) {
		t.Errorf("items should be equal, got: %v, want: %v", item, expectedItem)
	}
}

func TestCacheEviction(t *testing.T) {
	cache := NewStringCache(2)
	// initial set
	cache.Add("key1")
	cache.Add("key2")
	// move this element away from the least recently used.
	cache.Add("key1")
	cache.Add("key3")

	expectedCacheSize := int64(2)
	if int64(len(cache.items)) != expectedCacheSize {
		t.Errorf("cache size is not as expected, got: %d, want: %d", len(cache.items), expectedCacheSize)
	}

	if len(cache.items) > 2 {
		t.Errorf("there should be only two items in cache, got: %d", len(cache.items))
	}

	if _, ok := cache.items["key2"]; ok {
		t.Errorf("item with key: %s should not exist in cache", "key2")
	}
}
