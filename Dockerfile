FROM golang:alpine

RUN apk update && apk add bash git pkgconfig geoip-dev geoip gcc libc-dev

WORKDIR /go/src/github.com/m-lab/annotation-service
COPY . .

RUN go get cloud.google.com/go/pubsub

RUN go get github.com/prometheus/client_golang/prometheus

RUN go install -v ./...

RUN go build

RUN chmod -R a+rx /go/src/github.com/m-lab/annotation-service/annotation-service
ENTRYPOINT ["/go/src/github.com/m-lab/annotation-service/annotation-service"]
