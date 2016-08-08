#! /bin/bash

if [[ ! -e target/salted-fish-16.01.jar ]]
then
    echo 'jar file is not found. run mvn package first'
    exit -1
fi
./hbase-util.sh --clean
rm -rf mysql-db/data oracle-db/data hbase-db/data
~/bin/mvn test
