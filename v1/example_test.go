package v1_test

import (
	"context"
	"fmt"

	"github.com/cnuss/libraft"
)

// New returns an unconfigured Builder; configure it with the With* methods and
// finalize with the terminal Node. The returned Node carries the assembled
// config; the underlying raft node starts when WithPeers or WithoutPeers is
// called on it.
func ExampleNew() {
	node := libraft.New().
		WithContext(context.Background()).
		WithID(1).
		WithElectionTick(10).
		WithHeartbeatTick(1).
		Node()

	fmt.Println(node != nil)
	// Output: true
}
