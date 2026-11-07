FROM golang:1.27.2@sha256:820153096fda4de6ac1b7194d12e416b7dd8f4e9a33d17e0345b111b945162d8
# Installed during online preparation only. The finished image ID and tar
# checksum fix the actual tool/dependency bytes used by offline acceptance.
RUN apt-get update && apt-get install -y --no-install-recommends python3 jq iptables ca-certificates curl make git && rm -rf /var/lib/apt/lists/*
COPY kubectl helm kind docker-real /usr/local/bin/
COPY docker-guard.sh /usr/local/bin/docker
COPY runner.sh /offline-runner
RUN chmod 755 /usr/local/bin/kubectl /usr/local/bin/helm /usr/local/bin/kind /usr/local/bin/docker-real /usr/local/bin/docker /offline-runner
ENTRYPOINT ["/offline-runner"]
