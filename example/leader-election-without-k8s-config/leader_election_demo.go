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

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/khh403/leaderelection"
	resourcelock2 "github.com/khh403/leaderelection/resourcelock"
	clientv3 "go.etcd.io/etcd/client/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/klog"
)

func main() {
	klog.InitFlags(nil)

	var port int
	var etcdEndpoints, etcdCertFilePath string
	var etcdPrefix string
	var leaseLockName string
	var leaseLockId string

	flag.IntVar(&port, "port", 8080, "port to listen")
	flag.StringVar(&etcdEndpoints, "etcd-endpoints", "https://192.168.31.19:2379", "Comma-separated list of etcd endpoints")
	flag.StringVar(&etcdCertFilePath, "etcd-cert", "/etc/cert", "relative path of etcd cert files")
	flag.StringVar(&etcdPrefix, "etcd-prefix", "/registry/leaderelection/demo", "etcd prefix")
	flag.StringVar(&leaseLockName, "lease-lock-name", "lease-lock-name", "etcd lease lock name")
	flag.StringVar(&leaseLockId, "lease-lock-id", "", "etcd lease lock id")
	flag.Parse()

	if leaseLockId == "" {
		panic("lease-lock-id must provide!!\n")
	}

	tlsConfig, err := leaderelection.GetTlsConfig(etcdCertFilePath)
	if err != nil {
		panic(fmt.Sprintf("GetTlsConfig fail, error: %s\n", err.Error()))
	}

	etcdConfig := clientv3.Config{
		Endpoints:   []string{etcdEndpoints},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}

	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		panic(fmt.Sprintf("Error creating etcd client: %s\n", err.Error()))
	}
	defer etcdClient.Close()

	// use a Go context so we can tell the leaderelection code when we want to step down
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// listen for interrupts or the Linux SIGTERM signal and cancel our context, which
	// the leader election code will observe and step down
	stoppedCh := make(chan os.Signal, 1)
	defer close(stoppedCh)
	signal.Notify(stoppedCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stoppedCh
		klog.Info("Received termination, signaling shutdown")
		cancel()
	}()

	// Setup any /healthz checks we will want to use on port.
	var checks []healthz.HealthChecker
	var electionChecker *leaderelection.HealthzAdaptor
	electionChecker = leaderelection.NewLeaderHealthzAdaptor(time.Second * 20)
	checks = append(checks, electionChecker)

	mux := http.NewServeMux()
	healthz.InstallPathHandler(mux, "/healthz", checks...)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	go func() {
		klog.Infof("Start listening to %d", port)

		if err := server.ListenAndServe(); err != nil {
			klog.Fatalf("Error starting server: %v", err)
		}
	}()

	// complete your controller loop here
	run := func(ctx context.Context) {
		klog.Info("Controller loop...")

		select {}
	}

	klog.Infof("Run with leader election")

	// we use the Lease lock type since edits to Leases are less common and
	// fewer objects in the cluster watch "all Leases".
	lock := &resourcelock2.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseLockName,
			Namespace: etcdPrefix,
		},
		Client: etcdClient,
		LockConfig: resourcelock2.ResourceLockConfig{
			Identity: leaseLockId,
		},
	}

	// start the leader election code loop
	// 调用函数 leaderelection.RunOrDie() 进行选举循环
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: lock,
		// IMPORTANT: you MUST ensure that any code you have that
		// is protected by the lease must terminate **before**
		// you call cancel. Otherwise, you could have a background
		// loop still running and another process could
		// get elected before your background loop finished, violating
		// the stated goal of the lease.
		// 重要提示:你必须确保任何受租约保护内的代码都必须在你调用 cancel 函数之前终止。
		// 否则，您可能会有一个后台循环一直在运行，并且在您的后台循环结束之前，另一个进程可能被选上，这违反了租约的目标。
		ReleaseOnCancel: true,
		// 这个 lease lock 持有的时间，即 lease lock ttl
		LeaseDuration: 30 * time.Second,
		// 需要续约的超时时间
		RenewDeadline: 10 * time.Second,
		// 重试周期
		RetryPeriod: 3 * time.Second,
		// 回调函数
		Callbacks: leaderelection.LeaderCallbacks{
			// OnStartedLeading 当选举通过为master之后，需要运行的代码
			OnStartedLeading: func(ctx context.Context) {
				// current node get the lease lock and be elected as master
				// we're notified when we start - this is where you would usually put your code
				run(ctx)
			},
			OnStoppedLeading: func() {
				// current node lost the lease lock and be slave node
				// we can do cleanup here
				klog.Infof("lost: lease-lock-id")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				// we're notified when new leader elected
				if identity == "lease-lock-id" {
					// I just got the lock
					return
				}
				klog.Infof("new leader elected: %s", identity)
			},
		},
		WatchDog: electionChecker,
	})
}
