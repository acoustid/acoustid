FROM golang:alpine as builder
RUN mkdir /build
ADD . /build/
WORKDIR /build
RUN go build -mod vendor -o acoustid ./cmd/acoustid

FROM alpine
RUN adduser -S -D -H -h /home/acoustid acoustid
USER acoustid
COPY --from=builder /build/acoustid /usr/local/bin/acoustid
CMD ["/usr/local/bin/acoustid"]
