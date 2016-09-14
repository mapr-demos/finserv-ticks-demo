# Processing engine for NYSE Market data

This project provides a processing engine for ingesting real time streams of NYSE trades and quotes into MapR-DB.

This project handles the Daily Trades dataset described [here](http://www.nyxdata.com/Data-Products/Daily-TAQ). The schema for our data is detailed in Table 6, "Daily Trades File Data Fields", on page 26 of [Daily TAQ Client Specification (from December 1st, 2013)](http://www.nyxdata.com/doc/212759).

## Pre-requisites

To start, you need to get a MapR 5.1 running. You can install your own cluster or download a sandbox.

### Step 1: Create the stream

A *stream* is a collection of topics that you can manage together for security, default number or partitions, and time to leave for the messages.

Run the following command on your MapR cluster:

```
$ maprcli stream create -path /user/mapr/taq -produceperm p -consumeperm p -topicperm p -ttl 900
```

In that command we created the topic with public permission since we want to be able to run producers and consumers from remote computers. Verify the stream was created with this command:

```
maprcli stream info -path /user/mapr/taq
```

### Step 2: Create the topics

We only need one topic for this program. Topics are also created with the `maprcli` tool.

```
$ maprcli stream topic create -path /user/mapr/taq -topic trades -partitions 3
```

Verify the topic was created successfully with this command:

```
$ maprcli stream topic list -path /taq
topic            partitions  logicalsize  consumers  maxlag  physicalsize
trades           1           0            0          0       0
```

### Step 3: Create the MapR-DB table

Create the table that will be used to persist parsed TAQ records consumed off the stream.

```
$ maprcli table info -path /apps/taq
```

### Step 4: Compile and package up the example programs

Go back to the root directory where you have saved this source code and
compile and build the program like this:

```
$ cd nyse-taq-processing-pipline
$ mvn package
...
```

The JUnit tests can take a few minutes to complete. To build without them, run `mvn package -DskipTests` instead.

The project create a jar with all external dependencies ( `./target/nyse-taq-streaming-1.0-jar-with-dependencies.jar` )

### Step 5. Start the Spark to Hive Consumer

```
$ /opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingToHive /mapr/ian.cluster.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:trades
```

### Step 6: Run the Producer

You can install the [MapR Client](http://maprdocs.mapr.com/51/index.html#AdvancedInstallation/SettingUptheClient-client_26982445-d3e146.html) and run the application locally,
or copy the jar file on your cluster (any node).

For example copy the program to your server using scp:

```
scp ./target/nyse-taq-streaming-1.0-jar-with-dependencies.jar mapr@<YOUR_MAPR_CLUSTER>:/home/mapr
```

I prefer to use `rsync` instead of `scp` because it's faster:

```
rsync -vapr --progress --stats --partial target/nyse-taq-streaming-1.0-jar-with-dependencies.jar mapr@10.200.1.101:~/
```

The producer will send a large number of messages to `/taq:trades`. Since there isn't
any consumer running yet, nobody will receive the messages. 

Then run the Producer like this:

```java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run producer [source data file] [stream:topic]```

For example,

```
$ java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run producer data/taqtrade20131218 /usr/mapr/taq:trades 
Sent msg number 0
Sent msg number 1000
...
Sent msg number 998000
Sent msg number 999000
```

The command-line argument `data/taqtrade20131218` refers to the source file containing the TAQ dataset to be published into the `taq:trades` MapR stream.


### Step 7: Start the Consumer

In another window you can run the consumer using the following command:

```java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer [stream:topic] [num_threads]```

For example,

```
$ java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer /user/mapr/taq:trades 2
Sent msg number 0
Sent msg number 1000
...
Sent msg number 998000
Sent msg number 999000
```

### Step 8: Interactive Data Analysis (optional)

You can use Apache Zeppelin for interactive data analysis. We've included a sample Zeppelin notebook which includes some sample SQL queries and charts to get you started. The notebook is in zeppelin/notebook/2BW3AEAGG/note.json. Refer to [this blog](http://fedulov.website/2015/10/16/export-apache-zeppelin-notebooks/) for instructions on how to import a notebook into Zeppelin.

Our Zeppelin server is at [http://iannodec.westus.cloudapp.azure.com:7000](http://iannodec.westus.cloudapp.azure.com:7000).

### Monitoring your topics 

You can use the `maprcli` tool to get some information about the topic, for example:

```
$ maprcli stream info -path /user/mapr/taq -json
$ maprcli stream topic info -path /user/mapr/taq -topic trades -json
```

If the MapR-FS filesystem runs out of free space, the kafka producers will fail, so keep an eye on disk space like this:

```
$ maprcli disk list -host iannodea
```

If the disk space fills up, it's easiest just to remove the stream then create it again.


Show me all the topics for my stream:

```
$ maprcli stream topic list -path /user/mapr/taq | awk '{print $4}' | sort | uniq -c
```

Show me the depth of the trades topic:

```
$ maprcli stream topic info -path /user/mapr/taq -topic trades | tail -n 1 | awk '{print $12-$2}'
```

## Cleaning Up

When you are done, you can delete the stream, and all associated topic using the following command:

```
$ maprcli stream delete -path /taq
```

Don't forget to recreate the stream before running the producer again.

Remove the Hive table:

```
$ rm -rf /mapr/ian.cluster.com/user/hive/warehouse/streaming_ticks/
```

# Performance Guidelines

We suggest you use multiple partitions for the first stage of sending raw data to the taq:trades stream:topic, and use three consumer processes with two threads each for the middle stage of consuming that raw data and multiplexing it to receiver and sender topics.  In summary, create your topic like this:

``` 
$ maprcli stream create -path /user/mapr/taq -ttl 300
$ maprcli stream topic create -path /user/mapr/taq -topic trades -partitions 3
```

Then run the consumers on three different cluster nodes, with 2 threads each, like this:

```
time java -cp `mapr classpath`:/mapr/tmclust1/user/mapr/resources:/mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer2  /user/mapr/taq:trades 2
```

Then run the producer like this:

```
java -cp `mapr classpath`:/mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run producer nyse/1minute /user/mapr/taq:trades;
```


# Testing Speeds for Different Configurations
There are several unit tests that don't so much test anything as produce speed data
so that different configurations of producer threads can be adjusted to get optimal 
performance under different conditions. 

To run these tests do this in the top-level directory:

    mvn -e -Dtest=TopicCountGridSearchTest,ThreadCountSpeedTest test

This will create two data files, `thread-count.csv` and `topic-count.csv`. These files can be visualized 
by running an analysis script:

    Rscript src/test/R/draw-speed-graphs.r 

This will create PNG images with figures something like these that we
produced on our test cluster:


![Effect of thread count on performance](images/thread.png)

![Effect of buffer size on performance](images/topics.png)
