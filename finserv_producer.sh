#!/usr/bin/env bash
export DATA=/home/mapr/nyse/data/
export STREAM=/user/mapr/taq
export TOPIC=trades
export CLASSPATH=`mapr classpath`:/mapr/ian.cluster.com/user/mapr/nyse-taq-streaming-1.0.jar

while true; java -cp $CLASSPATH com.mapr.demo.finserv.Run producer $DATA/$line $STREAM:$TOPIC; done