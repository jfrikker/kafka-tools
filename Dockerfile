FROM rust:1.35.0-stretch as build

COPY ./ ./

RUN cargo build --release

RUN mkdir -p /build-out

RUN cp target/release/kafka-tools /build-out/

# Ubuntu 18.04
FROM debian:stretch

COPY --from=build /build-out/kafka-tools /

CMD bash
