FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod ./
COPY . .
RUN go mod tidy && go mod download && CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o events .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/events .
EXPOSE 8082
CMD ["./events"]
