FROM golang:alpine

RUN apk update && apk add bash git

WORKDIR /go/src/github.com/m-lab/annotation-service
COPY . .

RUN go install -v ./...

RUN go build

COPY annotation-service /annotation-service
RUN chmod -R a+rx /annotation-service
ENTRYPOINT ["/annotation-service"]
