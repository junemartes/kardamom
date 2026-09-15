#!/usr/bin/env bash
# The C++ twin of cc-x86-64-v3.sh; see that file.
exec "${KARDAMOM_REAL_CXX:-g++}" "$@" -march=x86-64-v3
