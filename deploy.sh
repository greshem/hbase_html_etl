#!/bin/bash

rm -rf target
mvn package

scp target/etl-0.0.1-SNAPSHOT.jar root@192.168.1.73:/home/hbase/jars/
