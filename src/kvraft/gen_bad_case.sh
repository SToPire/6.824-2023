#!/bin/bash

tmpfile=$(mktemp)

for i in $(seq 1 100); do
    echo "Test $i"
    go test -race -run "3A" > $tmpfile
    if [ $? -ne 0 ]; then
        cp "$tmpfile" badcase/output.$i
    fi
done