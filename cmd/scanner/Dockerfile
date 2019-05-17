FROM       alpine:3.8
RUN        apk add --no-cache ca-certificates
COPY       scanner /bin/scanner
EXPOSE     80
ENTRYPOINT [ "/bin/scanner" ]

ARG revision
LABEL org.opencontainers.image.title="scanner" \
      org.opencontainers.image.source="https://github.com/cortexproject/cortex/tree/master/cmd/scanner" \
      org.opencontainers.image.revision="${revision}"
