# Final image
FROM busybox
MAINTAINER Singlestore 
COPY --from=builder /go/prometheus-postgresql-adapter /
ENTRYPOINT ["/prometheus-postgresql-adapter"]
