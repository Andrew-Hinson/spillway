# Build from the repo root: docker build -f cmd/operator/Dockerfile .
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY internal ./internal
COPY cmd/operator ./cmd/operator
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/operator ./cmd/operator

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/operator /operator
EXPOSE 8080 8081
ENTRYPOINT ["/operator"]
