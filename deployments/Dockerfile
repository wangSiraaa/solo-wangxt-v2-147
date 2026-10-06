# Build stage: static binary, no CGO.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/stress ./cmd/stress

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/server /out/stress /usr/local/bin/
USER app
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/server"]
