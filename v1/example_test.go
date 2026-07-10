package v1_test

import (
	"fmt"

	"github.com/cnuss/libraft"
)

// New returns an unconfigured Builder; finalize with Build. Node assembly is
// not implemented yet, so Build returns a nil raft.Node for now.
func ExampleNew() {
	node := libraft.New().Build()

	fmt.Println(node == nil)
	// Output: true
}
