#! /bin/bash

RESULT="`ps -ef | grep -v grep | grep com.antsdb.saltedfish.server.SaltedFishMain`"
if [[ "$RESULT" == "" ]] ; then
	echo "error: antsdb was not running"
	exit -1
fi
PID=`echo $RESULT | awk '{ print $2 }'`
kill $PID
echo "stopped antsdb"
