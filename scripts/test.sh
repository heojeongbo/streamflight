#!/bin/sh
# Check formatting, vet, run the tests under the race detector, and hold
# coverage at 100%.
set -eu

cd "$(dirname "$0")/.."

# go vet does not check formatting, and this script is the only gate.
unformatted=$(gofmt -l .)
if [ -n "${unformatted}" ]; then
	echo "not gofmt'd:" >&2
	echo "${unformatted}" >&2
	exit 1
fi

go vet ./...
go test -race -covermode=atomic -coverprofile=cover.out ./...

# Concurrency bugs do not show up in a single run. Re-run without coverage,
# which is cheap because the build is already warm.
go test -race -count="${STREAMFLIGHT_TEST_COUNT:-5}" ./...

total=$(go tool cover -func=cover.out | awk '/^total:/ { print $NF }')
echo "coverage: ${total}"
if [ "${total}" != "100.0%" ]; then
	go tool cover -func=cover.out | grep -v '100.0%$' >&2
	echo "coverage must be 100.0%" >&2
	exit 1
fi
