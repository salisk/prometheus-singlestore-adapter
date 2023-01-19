# Final image
FROM busybox
MAINTAINER Singlestore 
COPY bin/prometheus-singlestore-adapter /
ENTRYPOINT ["/prometheus-singlestore-adapter"]
