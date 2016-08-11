#!/usr/bin/env bash
export DATA=/home/mapr/test1/finserv-ticks-demo/data
export STREAM=/user/mapr/taq
export TOPIC=trades
ls -1 $DATA | while read line; do java -cp `mapr classpath`:/home/mapr/nyse/nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run producer $DATA/$line $STREAM:$TOPIC; done
