#!/usr/bin/env bash
export STREAM=/user/mapr/taq
export TOPIC=trades
java -cp `mapr classpath`:/home/mapr/nyse/nyse-taq-streaming-1.0.jar com.mapr.demo.finserv.Run consumer $STREAM:$TOPIC
