#!/usr/bin/env bash
export STREAM=/user/mapr/taq
export TOPIC=trades
java -cp `mapr classpath` -jar nyse-taq-streaming-1.0.jar consumer $STREAM:$TOPIC
