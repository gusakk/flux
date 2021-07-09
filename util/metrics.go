// Copyright © 2019 Dell Inc. or its subsidiaries.
// All Rights Reserved.
// This software contains the intellectual property of Dell Inc. or is licensed to Dell Inc.
// from third parties. Use of this software and the intellectual property
// contained therein is expressly limited to the terms and conditions of the
// License Agreement under which it is provided by or on behalf of Dell Inc. or its subsidiaries.

package intern

import "github.com/prometheus/client_golang/prometheus"

// stringCacheMetrics holds metrics related to the string cache.
type stringCacheMetrics struct {
	itemsCount *prometheus.GaugeVec
}

func newStringCacheMetrics() *stringCacheMetrics {
	const (
		namespace = "flux"
		subsystem = "string_cache"
	)

	return &stringCacheMetrics{
		itemsCount: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "items_count",
			Help:      "Number of items in the string cache",
		}, []string{}),
	}
}

// PrometheusCollectors satisfies the prom.PrometheusCollector interface.
func (sm *stringCacheMetrics) PrometheusCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		sm.itemsCount,
	}
}
