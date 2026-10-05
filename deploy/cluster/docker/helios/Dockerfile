# The L1 light client (helios), from the pinned release binary. helios
# publishes no container image; the images role builds this one beside
# the service images and pins it by digest like them. The checksum is
# the release asset's sha256, so a changed asset fails the build.
FROM ubuntu:24.04

ARG HELIOS_VERSION=0.11.1
ARG HELIOS_SHA256=339bf4ce73073c53790e41e3217b6d91f0e5d8571132b9e88689997613162ddb

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

ADD --checksum=sha256:${HELIOS_SHA256} \
    https://github.com/a16z/helios/releases/download/${HELIOS_VERSION}/helios_linux_amd64.tar.gz /tmp/helios.tgz
RUN tar -xzf /tmp/helios.tgz -C /usr/local/bin helios && rm /tmp/helios.tgz && \
    /usr/local/bin/helios --version

RUN groupadd --gid 10001 kardamom && \
    useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin kardamom
USER 10001:10001

ENTRYPOINT ["/usr/local/bin/helios"]
