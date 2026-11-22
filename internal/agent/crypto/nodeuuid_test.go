package clustercrypto

import (
	"regexp"
	"testing"
)

// NodeUUIDPattern is the uuid half of a node id: anchored alone it takes the
// uuids ValidNode takes, and never a sid:.
func TestNodeUUIDPattern(t *testing.T) {
	alone := regexp.MustCompile(`^` + NodeUUIDPattern + `$`)
	for s, uuid := range map[string]bool{
		"0f0e0d0c-0b0a-4908-8706-050403020100":  true,
		"0F0E0D0C-0B0A-4908-8706-050403020100":  false,
		"0f0e0d0c0b0a490887060504030201000":     false,
		"0f0e0d0c-0b0a-4908-8706-0504030201000": false,
		"sid:12":                                false,
		"":                                      false,
	} {
		if alone.MatchString(s) != uuid {
			t.Errorf("%q: uuid %v", s, !uuid)
		}
		if uuid && !ValidNode(s) {
			t.Errorf("%q: a uuid ValidNode refuses", s)
		}
	}
	if !ValidNode("sid:12") {
		t.Error("sid: no longer a node id")
	}
}
