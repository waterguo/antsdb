#! /bin/bash

function check_instance() {
    local result="`ps -ef | grep -v grep | grep com.antsdb.saltedfish.server.SaltedFishMain`"
    if [[ "$result" != "" ]] ; then
        echo "error: antsdb is already running"
        exit -1
    fi
}

function find_home() {
	local PRG="$1"
	while [ -h "$PRG" ] ; do
	  ls=`ls -ld "$PRG"`
	  link=`expr "$ls" : '.*-> \(.*\)$'`
	  if expr "$link" : '/.*' > /dev/null; then
		PRG="$link"
	  else
		PRG="`dirname "$PRG"`/$link"
	  fi
	done
	local ANTSDB_HOME=`dirname "$PRG"`/..
	local result=`cd "$ANTSDB_HOME" && pwd`
	echo $result
}

check_instance
ANTSDB_HOME=$(find_home $0)
JVM_CP="$ANTSDB_HOME/lib/antsdb-all.jar"
if [[ "$ANTSDB_OPTS" == "" ]] ; then
    ANTSDB_OPTS="-Xmx4g -server -XX:+UseParNewGC -XX:+UseConcMarkSweepGC"
fi

echo "starting antsdb ..."
nohup java -cp "$JVM_CP" $ANTSDB_OPTS com.antsdb.saltedfish.server.SaltedFishMain $ANTSDB_HOME > $ANTSDB_HOME/logs/nohup.log &

