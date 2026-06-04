FROM golang:1.25-bookworm AS build

WORKDIR /src

RUN apt-get update \
	&& apt-get install -y --no-install-recommends libsystemd-dev pkg-config \
	&& rm -rf /var/lib/apt/lists/*

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=dev

RUN CGO_ENABLED=1 GOOS=linux go build \
	-trimpath \
	-ldflags="-s -w -X main.buildVersion=${VERSION} -X main.buildCommit=${COMMIT}" \
	-o /out/cluster \
	./cmd/cluster

FROM debian:bookworm-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates libsystemd0 \
	&& rm -rf /var/lib/apt/lists/* \
	&& mkdir -p /tmp \
	&& chmod 1777 /tmp \
	&& useradd --system --uid 65532 --home /nonexistent --shell /usr/sbin/nologin nonroot

COPY --from=build /out/cluster /cluster

ENV TMPDIR=/tmp
USER 65532:65532
ENTRYPOINT ["/cluster"]
