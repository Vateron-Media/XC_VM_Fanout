package clusteragent

// recschedule.go is slice 3 of recording→Go: the control plane that drives the
// slice-1 archive writer and the slice-2 DVR recorder from the Go agent, on the
// RunChores tick — a cron:vod-cadence scheduler that starts a recorder per due
// recording, and a reconcile that runs an archive worker per tv_archive stream
// on this node. It replaces PHP's cron:vod spawn and StreamProcess's archive spawn.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
)

// streamRecordings reads the recordings scheduled on this node for stream id
// (the replica record's data.recordings[], RECORDING_FIELDS), mapping each to a
// recording whose StreamID is id. nil when the record is missing/unreadable.
func (a *Agent) streamRecordings(id string) []recording {
	b, err := os.ReadFile(filepath.Join(streamsDir(a.ReplicaDir), id+".json"))
	if err != nil {
		return nil
	}
	var w struct {
		Data struct {
			Recordings []struct {
				ID         int64  `json:"id"`
				Start      int64  `json:"start"`
				End        int64  `json:"end"`
				Archive    int64  `json:"archive"`
				Status     int    `json:"status"`
				SourceID   int64  `json:"source_id"`
				StreamIcon string `json:"stream_icon"`
			} `json:"recordings"`
		} `json:"data"`
	}
	if json.Unmarshal(b, &w) != nil {
		return nil
	}
	sid, _ := strconv.ParseInt(id, 10, 64)
	var out []recording
	for _, r := range w.Data.Recordings {
		out = append(out, recording{
			ID: r.ID, StreamID: sid, Start: r.Start, End: r.End,
			Archive: r.Archive == 1, StreamIcon: r.StreamIcon, SourceID: r.SourceID, Status: r.Status,
		})
	}
	return out
}

// shouldStartRecording is NodeStreams::recordingsDue plus the archive start-gate:
// the recording is scheduled on this node, not already recording/done, and either
// airing now or an archive (catch-up) job — and an archive job is held until
// now >= end+65 so every archive minute file has been written.
func shouldStartRecording(r recording, serverID, now int64) bool {
	if r.SourceID != serverID || r.Status == 1 || r.Status == 2 {
		return false
	}
	airing := r.Start <= now && now < r.End
	if !airing && !r.Archive {
		return false
	}
	if r.Archive && now < r.End+65 {
		return false
	}
	return true
}
