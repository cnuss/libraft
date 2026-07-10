// Command basic is the smallest libraft example: build a raft.Node through the
// builder and print the result.
package main

import (
	"fmt"

	"github.com/cnuss/libraft"
)

func main() {
	node := libraft.New().Build()

	fmt.Printf("node: %v\n", node)
}
