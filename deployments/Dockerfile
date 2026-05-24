FROM golang:1.26-alpine AS builder

WORKDIR /build

COPY go.mod go.sum* ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /build/server \
    ./cmd/server

FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /build/server /app/server

EXPOSE 8080
ENTRYPOINT ["/app/server"]
