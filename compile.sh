#!/bin/bash
# FlexQL build script
# Usage: sh compile.sh

set -e

echo "Building FlexQL server..."
g++ -O3 -std=c++17 -march=native -pthread \
    flexql_server.cpp \
    -o server

echo "Building benchmark client..."
g++ -O2 -std=c++17 \
    flexql.cpp benchmark_flexql.cpp \
    -o benchmark

echo ""
echo "Done. Run in two terminals:"
echo "  Terminal 1:  ./server"
echo "  Terminal 2:  ./benchmark --unit-test"
echo "  Terminal 2:  ./benchmark <row_count>"
