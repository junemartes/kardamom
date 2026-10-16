#!/bin/sh
# Usage: aeron-jar.sh VERSION SHA256 JAR
#
# Puts aeron-all-VERSION.jar at JAR and checks its SHA-256. A JAR that
# exists already is checked and not downloaded. A missing JAR comes from
# Maven Central: first repo1.maven.org, then repo.maven.apache.org, the
# second official host of the same repository. A reset connection or an
# HTTP error costs a retry, not the build. -f keeps an HTTP error page
# from landing as the jar. The hash check catches a truncated or
# substituted file.
set -eu

version=$1
sha256=$2
jar=$3
path="io/aeron/aeron-all/${version}/aeron-all-${version}.jar"

fetch() {
    curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 20 \
        "https://$1/maven2/${path}" -o "${jar}"
}

[ -f "${jar}" ] || fetch repo1.maven.org || fetch repo.maven.apache.org
echo "${sha256}  ${jar}" | sha256sum -c -
