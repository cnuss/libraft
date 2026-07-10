// Command basic is the smallest libraft example: build a Node through the
// builder and print the result.
package main

import (
	"fmt"

	"github.com/cnuss/libraft"
)

func main() {
	node := libraft.New().Node()

	fmt.Printf("node: %T\n", node)
}
