FROM golang:1.24.5-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o p2p-debugger .

EXPOSE 8001

CMD ["./p2p-debugger"]
