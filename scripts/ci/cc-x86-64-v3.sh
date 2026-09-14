#!/usr/bin/env bash
# A C compiler wrapper for CI builds that other runners execute.
#
# rusteron's build script compiles the Aeron C library with -march=native
# (hardcoded in its CMAKE_C_FLAGS_RELEASE). A binary built on a wide-ISA
# runner then SIGILLs on a narrower one. GCC takes the last -march on the
# command line, so this wrapper appends a fixed level after every flag
# the build passes. x86-64-v3 (AVX2) holds on the whole runner fleet.
#
# Use: CC=scripts/ci/cc-x86-64-v3.sh CXX=scripts/ci/cxx-x86-64-v3.sh cargo build ...
exec "${KARDAMOM_REAL_CC:-gcc}" "$@" -march=x86-64-v3
