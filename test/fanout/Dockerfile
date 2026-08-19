# Fanout test runner: builds the daemon and runs the Go integration suite that
# spawns it (one daemon per test, with per-test flags) against the origin service.
# Build context is the REPOSITORY ROOT so both the daemon module and test/ are in.
FROM golang:1.21-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

# Prebuild the daemon binary the tests launch.
RUN CGO_ENABLED=0 go build -o /usr/local/bin/xc_fanout ./cmd/xc_fanout

ENV XC_FANOUT_BIN=/usr/local/bin/xc_fanout \
    FFMPEG=ffmpeg \
    ORIGIN=http://origin:8080

WORKDIR /app/test
# -count=1 disables the test cache; the suite talks to live processes.
CMD ["go", "test", "-tags", "integration", "-v", "-count=1", "-timeout", "10m", "./integration/..."]
