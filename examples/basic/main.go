// Command basic is the smallest libraft example: build a value through the
// generic builder and print the result.
package main

import (
	"fmt"

	"github.com/cnuss/libraft"
)

func main() {
	res := libraft.New[string]().
		WithName("greeting").
		WithValue("hello world").
		Build()

	fmt.Printf("%s: %s\n", res.Name, res.Value)
}
