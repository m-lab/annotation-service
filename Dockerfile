# The standard google cloud-sdk container that "just works" within GCE.
FROM google/cloud-sdk

RUN apt-get update && apt-get install -y libgeoip-dev bzr pkg-config lxc-dev
# Add the server to respond to HTTP requests at port 8080.

COPY annotation-service /annotation-service
RUN chmod -R a+rx /annotation-service
ENTRYPOINT ["/annotation-service"]
