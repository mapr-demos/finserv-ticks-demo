#!/usr/bin/env bash
export DATA=/home/mapr/nyse/data
export STREAM=/usr/mapr/taq
export TOPIC=taq
ls -1 $DATA/* | while read line; do java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run producer $DATA/$line $STREAM:$TOPIC; done