package com.mapr.demo.finserv;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.*;

public class Consumer {


    // Declare a new consumer.
    public static KafkaConsumer consumer;
    public static KafkaConsumer jsonconsumer;
    public static KafkaProducer producer;
    public static long json_messages_published = 0L;
    public static long raw_records_parsed = 0L;
    static long startTime;
    static long last_update;
    static HashSet<String> jsontopics = new HashSet<>();

    public static void main(String[] args) {
        Runtime runtime = Runtime.getRuntime();

        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer  /usr/mapr/taq:trades");

        }

        String topic =  args[1] ;
        System.out.println("Subscribed to : "+ topic);

        configureConsumer();
        configureProducer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);


        long pollTimeOut = 5000;  // milliseconds


        // https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
        // This paradigm is an "at least once delivery" guarantee.
        // TODO: Is it okay for the listener #1 to potentially persist duplicate messages?

        startTime = System.nanoTime();
        last_update = 0;
        boolean printme = false;
        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);

                if (records.count() == 0) {
                    if (printme) {
                        producer.flush();
                        System.out.println("No messages after " + pollTimeOut / 1000 + "s. Total raw consumed = " +
                                raw_records_parsed + ". Total JSON published " + json_messages_published);
                        System.out.println("JSON topics:");
                        jsontopics.forEach(t -> System.out.println("\t" + t));
                        printme = false;
                    }
                } else {
                    if (printme == false) {
                        raw_records_parsed = 0;
                        json_messages_published=0;
                        startTime = System.nanoTime();
                        last_update = 0;
                        printme = true;
                    }
                    for (ConsumerRecord<String, String> record : records) {
                        Tick json = new Tick(record.value());
                        raw_records_parsed++;
                        streamJSON(record.key(),json);


                        consumer.commitSync();
                    }

                    // Print performance stats once per second
                    if ((Math.floor(System.nanoTime() - startTime)/1e9) > last_update) {
                        last_update++;
                        Monitor.print_status(raw_records_parsed, 1, startTime);
                    }
                }
            }

        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            consumer.close();
            System.out.println("Consumed " + raw_records_parsed + " messages from stream.");
            System.out.println("Finished.");
        }

    }

    public static void streamJSON(String key, Tick json) {
        String jsontopic = "/user/mapr/taq:"+json.getSender();
        jsontopics.add(jsontopic);
        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(jsontopic, key, json.toString());
        // Non-blocking send. Callback invoked when request is complete.
        producer.send(rec,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        json_messages_published++;  // TODO: make this thread safe
                        if(e != null)
                            e.printStackTrace();
                    }
                });

        // Print performance stats once per second
        if ((Math.floor(System.nanoTime() - startTime)/1e9) > last_update)
        {
            last_update ++;
            producer.flush();
            long elapsedTime = System.nanoTime() - startTime;
            System.out.printf("JSON messages published = %d. Thruput = %.2f Kmsgs/sec\n", json_messages_published,
                    json_messages_published / ((double) elapsedTime / 1e9) / 1000);
        }

    }

    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    /* Set the value for configuration parameters.*/
    public static void configureConsumer() {
        Properties props = new Properties();
        props.put("enable.auto.commit","false");
        props.put("group.id", "mapr-workshop");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        jsonconsumer = new KafkaConsumer<String, String>(props);
    }

}