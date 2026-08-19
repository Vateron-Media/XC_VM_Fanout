# Origin: a fake live source (MPEG-TS + HLS) built from a short synthetic sample.
FROM golang:1.21-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY origin/ ./origin/
RUN CGO_ENABLED=0 go build -o /out/origin ./origin

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Generate the looping sample once, at build time, so startup is fast and the
# stream is identical on every run. ~20s of 640x360 H.264 + AAC in MPEG-TS with a
# 1s GOP (clean HLS segmentation and small daemon join snapshots).
RUN mkdir -p /data \
 && ffmpeg -y -hide_banner -loglevel error \
      -f lavfi -i testsrc2=size=640x360:rate=25 \
      -f lavfi -i sine=frequency=1000:sample_rate=48000 \
      -c:v libx264 -preset veryfast -tune zerolatency \
      -g 25 -keyint_min 25 -pix_fmt yuv420p \
      -c:a aac -b:a 64k -t 20 \
      -f mpegts /data/sample.ts

COPY --from=build /out/origin /usr/local/bin/origin
EXPOSE 8080
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD curl -fsS http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/origin", "-addr", ":8080", "-sample", "/data/sample.ts"]
