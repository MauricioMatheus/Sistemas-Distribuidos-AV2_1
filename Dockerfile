FROM golang:1.25-alpine
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
RUN apk update && apk add --no-cache curl
COPY *.go ./
RUN go build -o main .
CMD ["./main"]
