// SPDX-License-Identifier: AGPL-3.0-or-later
// XC_VM_Fanout — https://github.com/Vateron-Media/XC_VM_Fanout
// See LICENSE and LICENSE-ADDITIONAL-TERMS.md

package tsjoin

import (
	"testing"

	"github.com/Vateron-Media/XC_VM_Fanout/internal/tsfixture"
)

// avPMT builds a PMT declaring one H.264 video ES and one AAC audio ES — an
// ordinary television channel, as opposed to tsfixture.PMT, which declares
// video alone.
func avPMT(pmtPID, videoPID, audioPID int) []byte {
	p := make([]byte, PacketSize)
	p[0] = 0x47
	p[1] = byte((pmtPID>>8)&0x1f) | 0x40 // PUSI
	p[2] = byte(pmtPID)
	p[3] = 0x10 // payload only
	p[4] = 0x00 // pointer_field
	sec := p[5:]
	sec[0] = 0x02                                                  // table_id (PMT)
	sec[1], sec[2] = 0xb0, 0x17                                    // section_length = 23: sec[3]..the CRC
	sec[3], sec[4] = 0x00, 0x01                                    // program_number
	sec[5] = 0xc1                                                  // version, current_next
	sec[6], sec[7] = 0x00, 0x00                                    // section_number, last_section_number
	sec[8], sec[9] = byte(0xe0|(videoPID>>8)&0x1f), byte(videoPID) // PCR_PID
	sec[10], sec[11] = 0xf0, 0x00                                  // program_info_length = 0
	sec[12] = 0x1b                                                 // stream_type: H.264
	sec[13], sec[14] = byte(0xe0|(videoPID>>8)&0x1f), byte(videoPID)
	sec[15], sec[16] = 0xf0, 0x00 // ES_info_length = 0
	sec[17] = 0x0f                // stream_type: AAC
	sec[18], sec[19] = byte(0xe0|(audioPID>>8)&0x1f), byte(audioPID)
	sec[20], sec[21] = 0xf0, 0x00                               // ES_info_length = 0
	sec[22], sec[23], sec[24], sec[25] = 0xb6, 0x1f, 0x2c, 0x08 // CRC32
	for i := 26; i < len(sec); i++ {
		sec[i] = 0xff // stuffing
	}
	return p
}

// A stream carries audio for exactly as long as its PMT says so. The daemon
// reports "this stream has audio" to the supervisor, whose audio-loss rule
// restarts a channel that carries video but no audio; left sticky, that answer
// outlived the source it was true for. A failover to a genuinely video-only
// backup — a different encoder command, an upstream that dropped its audio PID
// — then looked like a channel whose audio had died, and the supervisor
// restarted it every audio_loss_sec, for ever, each restart landing on the same
// video-only source.
func TestASourceThatDropsItsAudioStopsDeclaringIt(t *testing.T) {
	s := New(1<<20, 40000)
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), avPMT(0x100, 0x101, 0x102)))
	if _, _, hasAudio := s.Counters(); !hasAudio {
		t.Fatal("a PMT declaring an AAC stream did not register as having audio")
	}

	// The source is replaced by one whose PMT declares video only.
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))
	if _, _, hasAudio := s.Counters(); hasAudio {
		t.Error("a video-only PMT still reports the stream as carrying audio: the supervisor will restart it for silence it cannot fix")
	}
}

// The mirror only follows a table it could actually read. A truncated or
// corrupt PMT says nothing about the stream, and must not be read as "the audio
// is gone" — that would switch the audio-loss rule off for a channel that does
// carry audio.
func TestAnUnreadablePMTDoesNotWithdrawTheAudioDeclaration(t *testing.T) {
	s := New(1<<20, 40000)
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), avPMT(0x100, 0x101, 0x102)))

	bad := avPMT(0x100, 0x101, 0x102)
	bad[5] = 0x00 // table_id is no longer a PMT
	s.Update(bad)
	if _, _, hasAudio := s.Counters(); !hasAudio {
		t.Error("an unreadable PMT withdrew the audio declaration")
	}

	short := avPMT(0x100, 0x101, 0x102)
	short[5+1], short[5+2] = 0xb0, 0x09 // section_length ends before any ES entry
	s.Update(short)
	if _, _, hasAudio := s.Counters(); !hasAudio {
		t.Error("a PMT with no readable ES entries withdrew the audio declaration")
	}
}

// Audio arriving on a PID the PMT has since withdrawn must not be counted as
// this stream's audio: the supervisor reads the count as "audio is flowing".
func TestAudioPacketsStopCountingOnceThePMTWithdrawsThePID(t *testing.T) {
	s := New(1<<20, 40000)
	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), avPMT(0x100, 0x101, 0x102)))
	// A payload-unit-start on the audio PID is what the counter counts.
	s.Update(tsfixture.PESStart(0x102, 0, 0x21))
	before, _, _ := s.Counters()
	if before == 0 {
		t.Fatal("no audio packets counted while the PMT declared the PID")
	}

	s.Update(tsfixture.Concat(tsfixture.PAT(0x100), tsfixture.PMT(0x100, 0x101)))
	s.Update(tsfixture.PESStart(0x102, 0, 0x21))
	if after, _, _ := s.Counters(); after != before {
		t.Errorf("audio packets counted on a withdrawn PID: %d -> %d", before, after)
	}
}
