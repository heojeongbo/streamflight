#!/bin/sh
# Vet, run the tests under the race detector, and hold coverage at 100%.
set -eu

cd "$(dirname "$0")/.."

go vet ./...
go test -race -covermode=atomic -coverprofile=cover.out ./...

total=$(go tool cover -func=cover.out | awk '/^total:/ { print $NF }')
echo "coverage: ${total}"
if [ "${total}" != "100.0%" ]; then
	go tool cover -func=cover.out | grep -v '100.0%$' >&2
	echo "coverage must be 100.0%" >&2
	exit 1
fi
