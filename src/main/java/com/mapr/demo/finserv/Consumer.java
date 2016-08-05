package com.mapr.demo.finserv;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import java.text.ParseException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
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

    private static JSONObject parse(String record) throws ParseException {
        if (record.length() < 71) {
            throw new ParseException("Expected line to be at least 71 characters, but got " + record.length(), record.length());
        }

        JSONObject trade_info = new JSONObject();
        trade_info.put("date", record.substring(0, 9));
        trade_info.put("exchange", record.substring(9, 10));
        trade_info.put("symbol root", record.substring(10, 16).trim());
        trade_info.put("symbol suffix", record.substring(16, 26).trim());
        trade_info.put("saleCondition", record.substring(26, 30).trim());
        trade_info.put("tradeVolume", record.substring(30, 39));
        trade_info.put("tradePrice", record.substring(39, 46) + "." + record.substring(46, 50));
        trade_info.put("tradeStopStockIndicator", record.substring(50, 51));
        trade_info.put("tradeCorrectionIndicator", record.substring(51, 53));
        trade_info.put("tradeSequenceNumber", record.substring(53, 69));
        trade_info.put("tradeSource", record.substring(69, 70));
        trade_info.put("tradeReportingFacility", record.substring(70, 71));
        if (record.length() >= 74) {
            trade_info.put("sender", record.substring(71, 75));

            JSONArray receiver_list = new JSONArray();
            int i = 0;
            while (record.length() >= 78 + i) {
                receiver_list.add(record.substring(75 + i, 79 + i));
                i += 4;
            }
            trade_info.put("receivers", receiver_list);
        }
        return trade_info;

    }

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
                        try {
                            JSONObject json = parse(record.value());
                            raw_records_parsed++;
                            streamJSON(record.key(),json);
                        } catch (ParseException e) {
                            System.err.println(e.getMessage());
                        }

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

    public static void streamJSON(String key, JSONObject json) {
        String jsontopic = "/user/mapr/taq:"+json.get("sender");
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