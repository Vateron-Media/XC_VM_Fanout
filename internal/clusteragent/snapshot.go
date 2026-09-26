package clusteragent

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
)

// conn_snapshot (plan, Phase 6): the whole registry, sent when MAIN answers a
// heartbeat with want_conn_snapshot because its store for this node drifted
// from the digest the heartbeat carried. It goes in chunks of SnapshotChunk
// records, numbered from 0; MAIN applies it with the last one. A chunk MAIN
// refuses as out of order (409 SNAP_GAP) ends this snapshot: MAIN asks again
// if the drift stays. A chunk refused as busy (503 RATE_LIMITED) goes again,
// with the same snap_id and seq, after retry_after_ms.
//
//	{snap_id, seq, last, records: [record, …]}

// SnapshotChunk is the most records one conn_snapshot call carries.
var SnapshotChunk = 1000

// SnapshotBusyRetries is how often one chunk is sent again while MAIN answers
// that it is busy (503 RATE_LIMITED) before the snapshot is given up.
var SnapshotBusyRetries = 20

// SendSnapshot sends the registry to MAIN. Only one runs at a time; a request
// while one is running is ignored.
func (a *Agent) SendSnapshot(ctx context.Context) error {
	if a.Registry == nil || !a.snapshotting.CompareAndSwap(false, true) {
		return nil
	}
	defer a.snapshotting.Store(false)
	records := a.Registry.Records()
	id := make([]byte, 8)
	rand.Read(id)
	snap := hex.EncodeToString(id)
	for seq := 0; seq == 0 || seq*SnapshotChunk < len(records); seq++ {
		end := min((seq+1)*SnapshotChunk, len(records))
		chunk := records[seq*SnapshotChunk : end]
		var out struct {
			Applied int `json:"applied"`
			Removed int `json:"removed"`
			Dropped int `json:"dropped"`
		}
		last := end == len(records)
		err := a.Client.Call(ctx, "conn_snapshot", map[string]any{"snap_id": snap, "seq": seq, "last": last, "records": chunk}, &out, false)
		for busy := 0; busy < SnapshotBusyRetries; busy++ {
			// MAIN is busy: the same chunk again (same snap_id and seq) when
			// it says. It keeps the chunks it took.
			w, ok := busyWait(err)
			if !ok {
				break
			}
			if !sleep(ctx, w) {
				return ctx.Err()
			}
			err = a.Client.Call(ctx, "conn_snapshot", map[string]any{"snap_id": snap, "seq": seq, "last": last, "records": chunk}, &out, false)
		}
		if err != nil {
			var d *Denial
			if errors.As(err, &d) && d.Reason == "SNAP_GAP" {
				a.logf("cluster: connection snapshot: MAIN lost the order; it asks again if it must")
			}
			return err
		}
		if last {
			a.logf("cluster: connection snapshot: %d records (%d applied, %d removed, %d dropped)", len(records), out.Applied, out.Removed, out.Dropped)
			return nil
		}
	}
	return nil
}
