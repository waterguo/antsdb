#!/bin/bash

OPTIONS="$ANTSDB_OPTS -server" 
echo $OPTIONS

mvn -q exec:exec -Dexec.executable=java -Dexec.async=true \
  -Dexec.args="-cp %classpath $OPTIONS com.antsdb.saltedfish.server.SaltedFishMain $*"
