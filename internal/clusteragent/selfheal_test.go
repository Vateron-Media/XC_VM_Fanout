package clusteragent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// releaseFor ports FanoutBinaryCommand::releaseFor: the tag this server installs
// — newest unless the fleet canary holds it to a pin.
func TestReleaseFor(t *testing.T) {
	tags := []string{"0.16.0", "0.15.6", "0.15.5"} // newest first, as the API lists
	cases := []struct {
		name                   string
		canary, server         int64
		pin, want              string
	}{
		{"no canary -> newest", 0, 3, "", "0.16.0"},
		{"this server is the canary -> newest", 3, 3, "", "0.16.0"},
		{"held, no pin -> nothing", 3, 7, "", ""},
		{"held, pin -> newest at or below the pin", 3, 7, "0.15.6", "0.15.6"},
		{"held, pin below all -> nothing", 3, 7, "0.15.4", ""},
	}
	for _, c := range cases {
		if got := releaseFor(tags, c.canary, c.server, c.pin); got != c.want {
			t.Errorf("%s: releaseFor=%q want %q", c.name, got, c.want)
		}
	}
	// Numeric compare, not lexical: 0.15.10 > 0.15.9.
	if got := releaseFor([]string{"0.15.10", "0.15.9"}, 3, 7, "0.15.9"); got != "0.15.9" {
		t.Errorf("numeric compare: got %q want 0.15.9", got)
	}
}

func TestListReleasesChannelFilter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `[{"tag_name":"0.16.0","prerelease":true,"draft":false},`+
			`{"tag_name":"0.15.6","prerelease":false,"draft":false},`+
			`{"tag_name":"0.15.5d","prerelease":false,"draft":true},`+
			`{"tag_name":"0.15.5","prerelease":false,"draft":false}]`)
	}))
	defer srv.Close()
	stable, err := listReleases(context.Background(), srv.Client(), srv.URL, "stable")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stable, []string{"0.15.6", "0.15.5"}) {
		t.Fatalf("stable drops prerelease + draft: got %v", stable)
	}
	beta, err := listReleases(context.Background(), srv.Client(), srv.URL, "beta")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(beta, []string{"0.16.0", "0.15.6", "0.15.5"}) {
		t.Fatalf("beta includes prerelease (not draft): got %v", beta)
	}
}
