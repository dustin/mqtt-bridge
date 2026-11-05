# Stage 1: Build the Haskell project
FROM haskell:9.10-bookworm AS builder

RUN apt-get update && apt-get install -y zlib1g zlib1g-dev

WORKDIR /usr/src/app

COPY mqtt-bridge.cabal ./
RUN cabal update && cabal build --only-dependencies

# Keep source changes in a separate layer from dependency resolution.  Repeating
# the package-wide build lets this step reuse the dependencies built above.
COPY Setup.hs ./
COPY app ./app
COPY src ./src
RUN cabal build && \
    install -Dm755 "$(cabal list-bin exe:mqtt-bridge)" /out/mqtt-bridge

# Stage 2: Create the final image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y zlib1g ca-certificates

WORKDIR /usr/local/bin

# Copy the built executable from the builder stage
COPY --from=builder /out/mqtt-bridge .

VOLUME /etc/mqtt-bridge
WORKDIR /etc/mqtt-bridge

COPY bridge.conf .

ENV LANG=C.UTF-8
ENV LC_ALL=C.UTF-8

ENTRYPOINT ["/usr/local/bin/mqtt-bridge"]
