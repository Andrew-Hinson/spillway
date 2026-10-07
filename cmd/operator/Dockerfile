# Build from the repo root:
#   docker build --build-arg VECTOR_VERSION=0.58.0 -f cmd/operator/Dockerfile .
# The operator validates rendered config with the same Vector version the
# aggregator runs (docs/adr/0001-validate-in-the-operator-image.md), so the
# Makefile passes VECTOR_VERSION from the one place it's pinned.
ARG VECTOR_VERSION=0.58.0
FROM timberio/vector:${VECTOR_VERSION}-distroless-static AS vector

FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal ./internal
COPY cmd/operator ./cmd/operator
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/operator ./cmd/operator

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=vector /usr/local/bin/vector /usr/local/bin/vector
COPY --from=build /out/operator /operator
EXPOSE 8080 8081
ENTRYPOINT ["/operator"]
