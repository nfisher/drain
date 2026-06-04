# syntax=docker/dockerfile:1
FROM debian:bookworm-slim

ARG TARGETARCH

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates libsystemd0 \
	&& rm -rf /var/lib/apt/lists/* \
	&& mkdir -p /tmp \
	&& chmod 1777 /tmp \
	&& useradd --system --uid 65532 --home /nonexistent --shell /usr/sbin/nologin nonroot

COPY --chmod=0555 dist/cluster-linux-${TARGETARCH} /cluster

ENV TMPDIR=/tmp
USER 65532:65532
ENTRYPOINT ["/cluster"]
