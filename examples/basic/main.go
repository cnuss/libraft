// Command basic is the smallest libraft example: blank-import the s3raft
// installer and report the environment variable that activates it.
//
// s3raft installs itself into etcd by machine-code monkey-patch, triggered by
// the blank import below plus the ETCD_S3LOG_URL environment variable. With
// the variable unset the installer is a no-op, so this program is safe to run
// as a smoke test — it just prints the seam.
package main

import (
	"fmt"

	v3 "github.com/cnuss/libraft/v3"
	_ "github.com/cnuss/libraft/v3/reflect"
)

func main() {
	fmt.Printf("s3raft installed; set %s to activate\n", v3.EnvURL)
}
