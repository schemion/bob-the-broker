FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/bobthebroker ./cmd/bobthebroker

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=build /out/bobthebroker /bobthebroker
EXPOSE 9092
USER nonroot:nonroot
ENTRYPOINT ["/bobthebroker"]
