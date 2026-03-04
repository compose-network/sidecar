FROM rust:1.91-alpine AS builder

RUN apk add --no-cache musl-dev protobuf-dev openssl-dev openssl-libs-static

WORKDIR /app
COPY . .
RUN cargo build --release -p sidecar

FROM alpine:3.21

RUN apk add --no-cache ca-certificates wget

COPY --from=builder /app/target/release/sidecar /usr/local/bin/sidecar
COPY --from=builder /app/configs /etc/sidecar/configs

EXPOSE 8090

ENTRYPOINT ["/usr/local/bin/sidecar"]
