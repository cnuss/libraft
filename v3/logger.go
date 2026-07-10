package v3

import (
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.uber.org/zap"
)

func Logger() *zap.Logger {
	lcfg := logutil.DefaultZapLoggerConfig
	lg, err := lcfg.Build()
	if err != nil {
		return zap.NewNop()
	}
	return lg.Named("s3raft")
}
