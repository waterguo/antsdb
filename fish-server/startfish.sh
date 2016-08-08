#!/bin/bash

OPTIONS="$ANTSDB_OPTS -server" 
# OPTIONS="$OPTIONS -XX:+PrintGCDetails"
echo $OPTIONS
java $OPTIONS -cp target/salted-fish-*.jar com.antsdb.saltedfish.server.SaltedFishMain $@

