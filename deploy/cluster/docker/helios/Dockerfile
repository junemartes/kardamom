# The L1 light client (helios), from the pinned release binary. helios
# publishes no container image; the images role builds this one beside
# the service images and pins it by digest like them. The checksum is
# the release asset's sha256, so a changed asset fails the build.
FROM ubuntu:24.04

ARG HELIOS_VERSION=0.12.0
ARG HELIOS_SHA256=dd2746b11323f8f4a4e4613891b275148f34963bf4912cab79a34316a02a94b4

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
