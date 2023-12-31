/*
Copyright (c) 2023 khh403

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
*/

package leaderelection

import (
	"sync"
)

// This file provides abstractions for setting the provider (e.g., prometheus)
// of metrics.

type leaderMetricsAdapter interface {
	leaderOn(name string)
	leaderOff(name string)
	slowpathExercised(name string)
}

// LeaderMetric instruments metrics used in leader election.
type LeaderMetric interface {
	On(name string)
	Off(name string)
	SlowpathExercised(name string)
}

type noopMetric struct{}

func (noopMetric) On(name string)                {}
func (noopMetric) Off(name string)               {}
func (noopMetric) SlowpathExercised(name string) {}

// defaultLeaderMetrics expects the caller to lock before setting any metrics.
type defaultLeaderMetrics struct {
	// leader's value indicates if the current process is the owner of name lease
	leader LeaderMetric
}

func (m *defaultLeaderMetrics) leaderOn(name string) {
	if m == nil {
		return
	}
	m.leader.On(name)
}

func (m *defaultLeaderMetrics) leaderOff(name string) {
	if m == nil {
		return
	}
	m.leader.Off(name)
}

func (m *defaultLeaderMetrics) slowpathExercised(name string) {
	if m == nil {
		return
	}
	m.leader.SlowpathExercised(name)
}

type noMetrics struct{}

func (noMetrics) leaderOn(name string)          {}
func (noMetrics) leaderOff(name string)         {}
func (noMetrics) slowpathExercised(name string) {}

// MetricsProvider generates various metrics used by the leader election.
type MetricsProvider interface {
	NewLeaderMetric() LeaderMetric
}

type noopMetricsProvider struct{}

func (noopMetricsProvider) NewLeaderMetric() LeaderMetric {
	return noopMetric{}
}

var globalMetricsFactory = leaderMetricsFactory{
	metricsProvider: noopMetricsProvider{},
}

type leaderMetricsFactory struct {
	metricsProvider MetricsProvider

	onlyOnce sync.Once
}

func (f *leaderMetricsFactory) setProvider(mp MetricsProvider) {
	f.onlyOnce.Do(func() {
		f.metricsProvider = mp
	})
}

func (f *leaderMetricsFactory) newLeaderMetrics() leaderMetricsAdapter {
	mp := f.metricsProvider
	if mp == (noopMetricsProvider{}) {
		return noMetrics{}
	}
	return &defaultLeaderMetrics{
		leader: mp.NewLeaderMetric(),
	}
}

// SetProvider sets the metrics provider for all subsequently created work
// queues. Only the first call has an effect.
func SetProvider(metricsProvider MetricsProvider) {
	globalMetricsFactory.setProvider(metricsProvider)
}
