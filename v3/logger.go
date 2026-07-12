package v3

import (
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.uber.org/zap"
)

// Logger builds a zap logger with etcd's default configuration. It does NOT
// apply the "libraft" name segment — the consumers that own log output do
// (Start, NewRaftNode, S3OpenBackend), so passing this straight into Start
// yields a single "libraft" segment rather than a double-named "libraft.libraft".
func Logger() *zap.Logger {
	lcfg := logutil.DefaultZapLoggerConfig
	lg, err := lcfg.Build()
	if err != nil {
		return zap.NewNop()
	}
	return lg
}
