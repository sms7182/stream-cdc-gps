FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

# Where Go downloads modules from. proxy.golang.org is blocked in some regions;
# override with a reachable proxy or "direct" (fetch from GitHub via git):
#   GOPROXY=direct docker compose build
ARG GOPROXY=https://proxy.golang.org,direct
ENV GOPROXY=${GOPROXY} \
    GOTOOLCHAIN=local

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd

FROM scratch
COPY --from=builder /app/main /app/main
ENTRYPOINT ["/app/main"]
