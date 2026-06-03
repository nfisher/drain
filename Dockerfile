FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=dev

RUN CGO_ENABLED=0 GOOS=linux go build \
	-trimpath \
	-ldflags="-s -w -X main.buildVersion=${VERSION} -X main.buildCommit=${COMMIT}" \
	-o /out/cluster \
	./cmd/cluster

RUN mkdir -p /out/tmp && chmod 1777 /out/tmp

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/cluster /cluster
COPY --from=build /out/tmp /tmp

ENV TMPDIR=/tmp
USER nonroot:nonroot
ENTRYPOINT ["/cluster"]
