# A copy of one pinned upstream image for the in-cluster registry of the
# container path. The images role builds it once for each entry of
# upstream_images in ansible/group_vars/all.yml and passes the reference
# as BASE. The copy keeps the layers, the user and the entrypoint of the
# upstream image, for the platform of the build host only.
ARG BASE
FROM ${BASE}
