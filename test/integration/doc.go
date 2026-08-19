// Package integration holds the end-to-end suite for the xc_fanout daemon.
//
// The real tests live in files tagged `integration` and are meant to run inside
// the docker-compose bench (see ../docker-compose.yml), where a fake source is
// reachable at $ORIGIN and the daemon binary at $XC_FANOUT_BIN. This untagged
// file exists so `go build`/`go vet ./...` on the test module doesn't fail with
// "no Go files" when the tag is absent.
package integration
