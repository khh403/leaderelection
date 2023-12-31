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
	"crypto/tls"
	"errors"
	"path/filepath"

	"go.etcd.io/etcd/pkg/transport"
)

func GetTlsConfig(filePath string) (config *tls.Config, err error) {
	if filePath == "" {
		return nil, errors.New("cert file path must provide")
	}

	tlsInfo := transport.TLSInfo{
		CertFile:      filepath.Join(filePath, "client.pem"),
		KeyFile:       filepath.Join(filePath, "client-key.pem"),
		TrustedCAFile: filepath.Join(filePath, "ca.pem"),
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}
	return tlsConfig, nil
}
