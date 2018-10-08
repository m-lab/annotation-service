FROM golang:alpine

RUN apk update && apk add bash git pkgconfig geoip-dev geoip gcc libc-dev

WORKDIR /go/src/github.com/m-lab/annotation-service
COPY . .
RUN go get github.com/m-lab/annotation-service
RUN apk del gcc libc-dev
RUN chmod -R a+rx /go/bin/annotation-service
ENTRYPOINT ["/go/bin/annotation-service"]
