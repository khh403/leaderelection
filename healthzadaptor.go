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
	"net/http"
	"sync"
	"time"
)

// HealthzAdaptor associates the /healthz endpoint with the LeaderElection object.
// It helps deal with the /healthz endpoint being set up prior to the LeaderElection.
// This contains the code needed to act as an adaptor between the leader
// election code the health check code. It allows us to provide health
// status about the leader election. Most specifically about if the leader
// has failed to renew without exiting the process. In that case we should
// report not healthy and rely on the kubelet to take down the process.
type HealthzAdaptor struct {
	pointerLock sync.Mutex
	le          *LeaderElector
	timeout     time.Duration
}

// Name returns the name of the health check we are implementing.
func (l *HealthzAdaptor) Name() string {
	return "leaderElection"
}

// Check is called by the healthz endpoint handler.
// It fails (returns an error) if we own the lease but had not been able to renew it.
func (l *HealthzAdaptor) Check(req *http.Request) error {
	l.pointerLock.Lock()
	defer l.pointerLock.Unlock()
	if l.le == nil {
		return nil
	}
	return l.le.Check(l.timeout)
}

// SetLeaderElection ties a leader election object to a HealthzAdaptor
func (l *HealthzAdaptor) SetLeaderElection(le *LeaderElector) {
	l.pointerLock.Lock()
	defer l.pointerLock.Unlock()
	l.le = le
}

// NewLeaderHealthzAdaptor creates a basic healthz adaptor to monitor a leader election.
// timeout determines the time beyond the lease expiry to be allowed for timeout.
// checks within the timeout period after the lease expires will still return healthy.
func NewLeaderHealthzAdaptor(timeout time.Duration) *HealthzAdaptor {
	result := &HealthzAdaptor{
		timeout: timeout,
	}
	return result
}
