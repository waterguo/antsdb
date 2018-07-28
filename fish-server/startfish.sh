#!/bin/bash

OPTIONS="$ANTSDB_OPTS -server" 
# OPTIONS="$OPTIONS -XX:+UnlockDiagnosticVMOptions -XX:CompileCommand=print,com/antsdb/saltedfish/cpp/FishSkipList*.*"
echo $OPTIONS

mvn -q exec:exec -Dexec.executable=java -Dexec.args="-cp %classpath $OPTIONS com.antsdb.saltedfish.server.SaltedFishMain $*"
