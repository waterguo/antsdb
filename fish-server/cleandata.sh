#! /bin/bash

hbase-stop
fish-shutdown
rm -rf mysql-db/data oracle-db/data hbase-db/data
rm -rf ~/opt/hbase/data ~/opt/hbase/zookeeper-data