package com.mapr.test;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;

import java.util.*;

public class BasicSparkStringConsumer {

    public static void main(String[] args) {

        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.test.BasicSparkStringConsumer target/nyse-taq-streaming-1.0-jar-with-dependencies.jar stream:topic\n" +
                    "EXAMPLE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.test.SparkStringConsumer /mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /mapr/tmclust1/user/mapr/taq:mytopic");
        }

        SparkConf conf = new SparkConf()
                .setAppName("Spark String Consumer")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));

        String topic = args[0];
        Set<String> topics = Collections.singleton(topic);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");

        //JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
        //        String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
            rdd.foreach(record -> System.out.println(record._2));
        });

        // TODO: test getting offsets from another topic, to use for fetching a subset of a topic
        // TODO: test using spark sql to query in the RDD

        ssc.start();
        ssc.awaitTermination();
    }

}