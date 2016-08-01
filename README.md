# Processing engine for NYSE Market data

This project provides a processing engine for ingesting real time streams of NYSE trades and quotes into MapR-DB.

This project handles the Daily TAQ (Trades and Quotes) dataset, described [here] (http://www.nyxdata.com/Data-Products/Daily-TAQ).

## Pre-requisites

To start, you need to get a MapR 5.1 running. You can install your own cluster or download a sandbox.

### Step 1: Create the stream

A *stream* is a collection of topics that you can manage together for security, default number or partitions, and time to leave for the messages.

Run the following command on your MapR cluster:

```
$ maprcli stream create -path /user/mapr/taq -produceperm p -consumeperm p -topicperm p -ttl 900
```

In that command we created the topic with public permission since we want to be able to run producers and consumers from remote computers.


### Step 2: Create the topics

We only need one topic for this program. Topics are also created with the `maprcli` tool.

```
$ maprcli stream topic create -path /user/mapr/taq -topic trades -partitions 3
```

Now, you can list the topic like this to make sure it was created:

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

The project create a jar with all external dependencies ( `./target/nyse-taq-streaming-1.0-jar-with-dependencies.jar` )


### Step 5: Run the Producer

You can install the [MapR Client](http://maprdocs.mapr.com/51/index.html#AdvancedInstallation/SettingUptheClient-client_26982445-d3e146.html) and run the application locally,
or copy the jar file on your cluster (any node).

For example copy the program to your server using scp:

```
scp ./target/nyse-taq-streaming-1.0-jar-with-dependencies.jar mapr@<YOUR_MAPR_CLUSTER>:/home/mapr
```


The producer will send a large number of messages to `/taq:trades`. Since there isn't
any consumer running yet, nobody will receive the messages. 

Then run the Producer like this:

```java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]```

For example,

```
$ java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer data/taqtrade20131218 /usr/mapr/taq:trades 
Sent msg number 0
Sent msg number 1000
...
Sent msg number 998000
Sent msg number 999000
```

The command-line argument `data/taqtrade20131218` refers to the source file containing the TAQ dataset to be published into the `taq:trades` MapR stream.


### Step 6: Start the Consumer

In another window you can run the consumer using the following command:

```java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]```

For example,

```
$ java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer /user/mapr/taq:trades
Sent msg number 0
Sent msg number 1000
...
Sent msg number 998000
Sent msg number 999000
```


### Monitoring your topics 

You can use the `maprcli` tool to get some information about the topic, for example:

```
$ maprcli stream info -path /user/mapr/taq -json
$ maprcli stream topic info -path /user/mapr/taq -topic trades -json
```


## Cleaning Up

When you are done, you can delete the stream, and all associated topic using the following command:
```
$ maprcli stream delete -path /taq
```
