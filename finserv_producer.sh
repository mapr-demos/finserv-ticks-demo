#!/usr/bin/env bash
export DATA=/home/mapr/nyse/data/
export STREAM=/user/mapr/taq
export TOPIC=trades
export CLASSPATH=`mapr classpath`

while true; java -cp $CLASSPATH -jar nyse-taq-streaming-1.0.jar producer $DATA/$line $STREAM:$TOPIC; done