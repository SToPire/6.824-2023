#!/bin/bash

tmpfile=$(mktemp)

for i in $(seq 1 1000); do
    echo "Test $i"
    go test -run 2A > $tmpfile
    if [ $? -ne 0 ]; then
        cp "$tmpfile" badcase/output.$i
    fi
done