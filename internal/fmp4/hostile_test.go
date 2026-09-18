package fmp4

import "testing"

// A malformed esds — an esds whose ES_Descriptor declares a length shorter than its own fixed
// header. findDescriptor sliced b[off+3:off+size] with size<3 → low>high panic.
func TestHostileESDSDoesNotPanic(t *testing.T) {
	// mp4a entry (28 bytes) + esds box whose descriptor tree is malformed.
	esdsBody := []byte{0, 0, 0, 0, // fullbox version/flags
		0x03, 0x02, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE} // ES_Descriptor tag 0x03, size 2 (<3), but ≥3 bytes remain
	esds := mkBox("esds", esdsBody)
	mp4a := mkBox("mp4a", append(make([]byte, 28), esds...))
	stsd := mkBox("stsd", []byte{0, 0, 0, 0}, u32(1), mp4a)
	trak := mkBox("trak",
		mkBox("tkhd", []byte{0, 0, 0, 1}, u32(0), u32(0), u32(2), make([]byte, 60)),
		mkBox("mdia",
			mkBox("mdhd", []byte{0, 0, 0, 0}, u32(0), u32(0), u32(48000), u32(0), make([]byte, 4)),
			mkBox("hdlr", []byte{0, 0, 0, 0}, u32(0), []byte("soun"), make([]byte, 12)),
			mkBox("minf", mkBox("stbl", stsd))))
	body := append(mkBox("ftyp", []byte("isom")), mkBox("moov", trak)...)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ParseInit PANICKED (C1): %v", r)
		}
	}()
	_, _ = ParseInit(body) // must return an error, not panic
}

// A hostile trun sample_count — a trun with a huge sample_count, no per-sample fields, and default size 0
// appends billions of zero-length samples → OOM. Must be refused.
func TestHostileSampleCountIsRefused(t *testing.T) {
	in := &Init{Tracks: []Track{{ID: 1, Kind: KindVideo, TimeScale: 90000, NALULengthSize: 4}}}
	tfhd := mkBox("tfhd", []byte{0, 0, 0, 0}, u32(1))
	tfdt := mkBox("tfdt", []byte{1, 0, 0, 0}, u64(0))
	// trun flags = 0 (no data-offset, no per-sample anything), count = 100 million.
	trunBody := append([]byte{0, 0, 0, 0}, u32(4_000_000_000)...) // ~4.29e9 fits; > mdat → refuse fast
	moof := mkBox("moof", mkBox("mfhd", []byte{0, 0, 0, 0}, u32(1)),
		mkBox("traf", tfhd, tfdt, mkBox("trun", trunBody)))
	seg := append(moof, mkBox("mdat", []byte{1, 2, 3})...)

	f, err := ParseFragment(seg, in)
	if err == nil {
		t.Fatalf("C3: a 100M-sample trun over a 3-byte mdat was accepted (%d samples)", len(f.Video))
	}
}

// A targeted fuzz over the fields that carry their own lengths — box sizes,
// descriptor lengths, sample counts, byte offsets — since those are what a
// hostile init or media segment abuses. A panic here runs on the pull
// goroutine and takes the whole daemon down, so the only acceptable outcome for
// any input is an error.
func TestHostileFuzzNeverPanics(t *testing.T) {
	seeds := [][]byte{initSegment(), mediaSegment(0, [][]byte{avcc([]byte{0x65, 1})}, 3000, nil, 0)}
	in := &Init{Tracks: []Track{
		{ID: 1, Kind: KindVideo, TimeScale: 90000, NALULengthSize: 4, SPS: [][]byte{{1}}, PPS: [][]byte{{2}}},
		{ID: 2, Kind: KindAudio, TimeScale: 48000, AACProfile: 1, SampleRateIdx: 3, Channels: 2},
	}}
	rng := uint32(2166136261)
	next := func(n int) int { rng = rng*16777619 + 1; return int(rng>>8) % n }
	for i := 0; i < 120000; i++ {
		b := append([]byte(nil), seeds[i%len(seeds)]...)
		// corrupt a handful of bytes — enough to hit length/count/offset fields
		for k := 0; k < 6 && len(b) > 0; k++ {
			b[next(len(b))] = byte(rng)
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on %x: %v", b, r)
				}
			}()
			_, _ = ParseInit(b)
			_, _ = ParseFragment(b, in)
		}()
	}
}
