FROM golang:1.26.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go test ./... && CGO_ENABLED=0 go build -o /out/goaria ./cmd/goaria

FROM scratch
COPY --from=build /out/goaria /goaria
ENTRYPOINT ["/goaria"]
