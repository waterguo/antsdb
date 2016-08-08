#!/bin/bash

java $OPTIONS -cp target/salted-fish-*.jar  com.antsdb.saltedfish.storage.HBaseUtilMain "$@"

