FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/gossip ./cmd

# Pre-create /app dir
RUN mkdir -p /out/app

# Pre-create the data dir owned by the runtime UID so named
# volumes are seeded with correct permissions on first use.
RUN mkdir -p /out/data/logs && chown -R 10001:10001 /out/data

FROM scratch

COPY --from=builder /out/gossip /app/gossip
# copy to create an empty /data/logs dir with the right ownership
COPY --from=builder /out/data   /data

USER 10001:10001

EXPOSE 7950
VOLUME ["/data"]

CMD ["/app/gossip", "-l", ":7950", "/data/logs"]
