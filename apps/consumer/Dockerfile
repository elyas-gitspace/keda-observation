# --- Étape 1 : build ---
FROM golang:1.22-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o consumer .

# --- Étape 2 : image finale ---
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/consumer /consumer

USER nonroot:nonroot

EXPOSE 9090

ENTRYPOINT ["/consumer"]
