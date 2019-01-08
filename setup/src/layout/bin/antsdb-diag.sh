#! /bin/bash

echo 'kernal version: ' `uname -a`
echo 'java version: ' `java -version 2>&1 | grep VM`
echo 'huge page: ' `cat /sys/kernel/mm/transparent_hugepage/enabled`
echo 'numa: ' `dmesg | grep -i numa`
echo 'max open files: ' `ulimit -n`
echo 'max cpu time: ' `ulimit -t`
echo 'max virtual memory: ' `ulimit -v`
echo 'max memory size: ' `ulimit -m`
echo 'max threads: ' `ulimit -u`