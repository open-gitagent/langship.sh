# API server only — no UI, no web build stage.
# The UI lives in its own container (web/Dockerfile) and proxies /api here.

FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/flow ./cmd/flow

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/flow /usr/local/bin/flow
EXPOSE 8080 9080
ENTRYPOINT ["/usr/local/bin/flow"]
CMD ["serve"]
