#!/usr/bin/env bash
export DATA=/home/mapr/test1/finserv-ticks-demo/data
export STREAM=/mapr/tmclust1/user/mapr/taq
export TOPIC=raw
ls -1 $DATA | while read line; do java -cp `mapr classpath`:./target/nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run producer $DATA/$line $STREAM:$TOPIC; done
