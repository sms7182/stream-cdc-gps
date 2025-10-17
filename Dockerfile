FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR /app

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main ./cmd

FROM scratch
COPY --from=builder /app/main /app/main
ENTRYPOINT ["/app/main"]