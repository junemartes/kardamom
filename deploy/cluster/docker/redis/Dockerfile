# Redis for the account-state projection and the receipt index. One
# image serves the primary, the replica, and the sentinels; the Nomad job
# picks the role by its command line. Built through the same digest-pin
# and manifest path as every service image, so cluster-e2e validates it.
#
# The version is pinned. Sentinel hostname resolution
# (`sentinel resolve-hostnames`) needs Redis 6.2 or newer.
FROM redis:7.4-alpine

COPY redis.conf /usr/local/etc/redis/redis.conf

# The default entrypoint runs `redis-server`; the job passes the config
# path and the role flags as arguments.
