#!/bin/bash

# java $OPTIONS -cp target/salted-fish-*.jar com.antsdb.saltedfish.sql.FishMetaUtilMain "$@"

mvn -q exec:java -Dexec.mainClass=com.antsdb.saltedfish.sql.FishMetaUtilMain -Dexec.args="$*"
