// Copyright © 2019 Dell Inc. or its subsidiaries.
// All Rights Reserved.
// This software contains the intellectual property of Dell Inc. or is licensed to Dell Inc.
// from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the
// License Agreement under which it is provided by or on behalf of Dell Inc. or its subsidiaries.

package intern

// Pool of strings which will be used for string interning.
// As flux structures all data in the tables, each table row may contain a lot of duplicated strings
// (tags, measurement, e.t.c). A lot of memory is allocated for the same strings and lead flux to consume
// a lot of memory. So, to reduce amount of memory used by the flux engine, it was decided to implement a pool of strings
// to not allocate additional memory if the string already exists in the pool. So the memory for the same strings will be allocated once.
// The pool is limited by 100.000 elements, so it means that the least recently used elements from the pool
// will be removed if the pool size will be exceeded.
var pool Cache = &NoopStringCache{}

// RegisterStringCache registers string cache with the specified maxCacheSize
// to save duplicated strings.
func RegisterStringCache(maxCacheSize int64) Cache {
	pool = NewStringCache(maxCacheSize)
	return pool
}

// String returns the string from the string pool.
func String(s string) string {
	if len(s) > 0 {
		if res, ok := pool.Get(s); ok {
			return res
		}
	}
	return s
}

// InternString pushes the string to the underlying string cache.
func InternString(s string) string {
	if len(s) > 0 {
		if res, ok := pool.Get(s); ok {
			return res
		}
		return pool.Add(s)
	}
	return s
}
