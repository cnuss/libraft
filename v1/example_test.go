package v1_test

import (
	"fmt"
	"log/slog"

	"github.com/cnuss/libraft"
)

// New returns an unconfigured Builder; configure it with the With* methods and
// finalize with Build. Node assembly is not implemented yet, so Build returns
// a nil raft.Node for now.
func ExampleNew() {
	node := libraft.New().
		WithLog(slog.Default()).
		Build()

	fmt.Println(node == nil)
	// Output: true
}
