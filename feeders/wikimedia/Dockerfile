# Build from the repo root: docker build -f feeders/wikimedia/Dockerfile .
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY feeders/wikimedia ./feeders/wikimedia
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/feeder ./feeders/wikimedia

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/feeder /feeder
EXPOSE 8080
ENTRYPOINT ["/feeder"]
