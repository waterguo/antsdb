#!/bin/bash

mvn -q exec:exec -Dexec.executable=java -Dexec.async=true \
  -Dexec.args="-cp %classpath com.antsdb.saltedfish.sql.FishFindMain $*"
