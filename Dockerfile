FROM golang:1.15-alpine as build

RUN apk add --no-cache git pkgconfig geoip-dev geoip gcc libc-dev
ADD . /go/src/github.com/m-lab/annotation-service
WORKDIR /go/src/github.com/m-lab/annotation-service
RUN go get \
      -v \
      -ldflags "-X github.com/m-lab/go/prometheusx.GitShortCommit=$(git log -1 --format=%h)" \
      ./...
RUN chmod -R a+rx /go/bin/annotation-service

FROM golang:alpine
RUN apk add --no-cache geoip
COPY --from=build /go/bin/annotation-service /
COPY --from=build /go/src/github.com/m-lab/annotation-service/data /data
WORKDIR /

ENTRYPOINT ["/annotation-service"]
