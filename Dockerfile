FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -o /out/gossip ./cmd/main.go

FROM alpine:3.22

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/gossip /app/gossip

RUN mkdir -p /data/logs

EXPOSE 7950
VOLUME ["/data"]

CMD ["/app/gossip", "-l", ":7950", "/data/logs"]
