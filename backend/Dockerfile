FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/api ./cmd/api
RUN CGO_ENABLED=0 go build -o /out/loadtest ./cmd/loadtest

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/api /api
COPY --from=build /out/loadtest /loadtest
EXPOSE 8080
ENTRYPOINT ["/api"]
