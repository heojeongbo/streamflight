group "default" {
  targets = ["test"]
}

target "test" {
  target = "test"
  output = ["type=cacheonly"]
  # A passing run is not something to reuse: the same tree tested again must
  # run the tests again, which repeat to find what one run can miss.
  no-cache-filter = ["test"]
}
