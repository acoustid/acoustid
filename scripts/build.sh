#!/usr/bin/env bash

set -eux

cd $(dirname $0)/../

for name in cmd/*; do
  go build -o bin/$(basename $name) ./$name
done
