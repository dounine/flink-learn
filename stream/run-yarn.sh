#!/bin/bash
flink run -d -m yarn-cluster -ynm "stream" -yst -yn 4 -p 4 -ys 1 -ytm 2048 ./build/libs/stream*.jar