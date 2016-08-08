#!/bin/bash

# java $OPTIONS -cp target/salted-fish-*.jar com.antsdb.saltedfish.sql.FishFindMain "$@"

mvn -q exec:java -Dexec.mainClass=com.antsdb.saltedfish.sql.FishFindMain -Dexec.args="$*"
