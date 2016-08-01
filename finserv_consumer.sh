#!/usr/bin/env bash
export STREAM=/mapr/tmclust1/user/mapr/taq
export TOPIC=raw
java -cp `mapr classpath`:./target/nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer $STREAM:$TOPIC
