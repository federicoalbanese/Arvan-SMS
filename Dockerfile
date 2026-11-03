# Build stage: compile static binaries.
FROM golang:1.26-alpine AS build
WORKDIR /src

# Download dependencies first so this layer is cached between code changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# Runtime stage: small image, non-root user.
FROM alpine:3.22
RUN adduser -D -u 10001 app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080 9090
CMD ["/app/api"]
