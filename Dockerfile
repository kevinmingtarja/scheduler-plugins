FROM golang:1.26.5 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY pkg ./pkg
RUN CGO_ENABLED=0 go build -o /maint-scheduler ./cmd/scheduler

FROM scratch
COPY --from=build /maint-scheduler /maint-scheduler
USER 65532:65532
ENTRYPOINT ["/maint-scheduler"]
