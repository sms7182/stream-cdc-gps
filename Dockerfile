FROM golang:alpine as builder

RUN apk update && apk add --no-cache git

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o main

FROM scratch

COPY --from=builder /app/cmd/main /app/cmd/main

ENTRYPOINT ["/app/cmd/main"]