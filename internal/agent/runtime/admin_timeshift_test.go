package clusteragent

import (
	"testing"
	"time"
)

func TestTimeshiftStartTimestamp(t *testing.T) {
	utc := time.UTC
	// numeric → the int itself.
	if got := timeshiftStartTimestamp("1700000000", utc); got != 1700000000 {
		t.Fatalf("numeric: %d", got)
	}
	// Y-m-d:H-i → mktime(year,month,day,hour,min) in the panel timezone.
	wantA := time.Date(2026, 10, 21, 14, 30, 0, 0, utc).Unix()
	if got := timeshiftStartTimestamp("2026-10-21:14-30", utc); got != wantA {
		t.Fatalf("Y-m-d:H-i: %d, want %d", got, wantA)
	}
	// YYYYMMDD-H → that date at hour H, minute 0.
	wantB := time.Date(2026, 10, 21, 14, 0, 0, 0, utc).Unix()
	if got := timeshiftStartTimestamp("20261021-14", utc); got != wantB {
		t.Fatalf("YYYYMMDD-H: %d, want %d", got, wantB)
	}
	// a non-UTC panel timezone shifts the wall-clock interpretation.
	loc, err := time.LoadLocation("Europe/Moscow")
	if err == nil {
		wantC := time.Date(2026, 10, 21, 14, 30, 0, 0, loc).Unix()
		if got := timeshiftStartTimestamp("2026-10-21:14-30", loc); got != wantC {
			t.Fatalf("tz: %d, want %d", got, wantC)
		}
	}
	// garbage → 0.
	if got := timeshiftStartTimestamp("not-a-date-at-all", utc); got != 0 {
		t.Fatalf("garbage: %d, want 0", got)
	}
}
