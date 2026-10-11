# Build from the repo root: docker build -f feeders/loadgen/Dockerfile .
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY feeders/loadgen ./feeders/loadgen
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/loadgen ./feeders/loadgen

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/loadgen /loadgen
EXPOSE 8080
ENTRYPOINT ["/loadgen"]
