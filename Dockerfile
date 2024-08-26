FROM rust:slim-bullseye as build
RUN apt-get update && apt-get install -y curl
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bullseye-slim 
COPY --from=build /app/target/release/mauved /usr/local/bin/mauved
COPY --from=build /app/mauve.yaml /etc/mauve.yaml

EXPOSE 9000

CMD ["/usr/local/bin/mauved", "-c", "/etc/mauve.yaml"]
