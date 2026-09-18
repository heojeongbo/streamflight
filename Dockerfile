FROM golang:1.26 AS base

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .



FROM base AS test

RUN --mount=type=cache,target=/root/.cache/go-build \
	./scripts/test.sh
