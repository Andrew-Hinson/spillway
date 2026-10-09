# Build from the repo root: docker build -f feeders/fixtures/Dockerfile .
FROM golang:1.27.1-trixie AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY feeders/fixtures ./feeders/fixtures
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/fixtures ./feeders/fixtures

FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/fixtures /fixtures
EXPOSE 8080
ENTRYPOINT ["/fixtures"]
