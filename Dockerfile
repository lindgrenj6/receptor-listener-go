FROM golang:1.14 AS builder
MAINTAINER jlindgre@redhat.com

WORKDIR /root
COPY . /root/
RUN go mod download && GOOS=linux go build -o kafka .

# FROM alpine
# WORKDIR /root
# COPY --from=builder /root/kafka .
ENTRYPOINT ["/root/kafka"]
