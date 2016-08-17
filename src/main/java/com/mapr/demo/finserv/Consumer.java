package com.mapr.demo.finserv;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Consumer implements Runnable {
    private static final long POLL_INTERVAL = 5000;  // milliseconds
    private static int NUM_THREADS = 1;

    private static long json_messages_published = 0L;
    private static long raw_records_parsed = 0L;

    private static long start_time;
    private static TreeSet<String> sender_topics = new TreeSet<>();
    private static TreeSet<String> receiver_topics = new TreeSet<>();

    private KafkaConsumer consumer;
    private KafkaProducer producer;
    private String topic;

    static ConcurrentHashMap<Tuple, OffsetTracker> offset_cache = new ConcurrentHashMap<>();

    private long my_json_messages_published = 0L;
    private long my_last_update = 0;

    public Consumer(String topic) {
        this.topic = topic;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer [stream:topic] [NUM_THREADS]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer /usr/mapr/taq:trades 2");

        }

        String topic = args[1];
        System.out.println("Subscribed to : " + topic);
        if (args.length == 3)
            NUM_THREADS = Integer.valueOf(args[2]);

        start_time = System.nanoTime();
        System.out.println("Spawning " + NUM_THREADS + " consumer threads");
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < NUM_THREADS; i++)
            threads.add(new Thread(new Consumer(topic)));
        threads.add(new Thread(new FiveMinuteTimer()));
        threads.forEach(thread -> thread.start());

        // Sometimes threads encounter a fatal exception. If that happens, create a new worker thread.
        while(true) {
            for (int i = 0; i < threads.size(); i++) {
                if (!(threads.get(i).isAlive())) {
                    System.out.println("Replacing dead thread.");
                    threads.set(i, new Thread(new Consumer(topic)));
                    threads.get(i).start();
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    /* Set the value for configuration parameters.*/
    private void configureConsumer() {
        Properties props = new Properties();
        props.put("enable.auto.commit","true");
        props.put("group.id", "mapr-workshop");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

    @Override
    public void run() {
        // This consumer uses an "at least once delivery" guarantee.
        //   https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
        // TODO: Is it okay for the listener #1 to potentially persist duplicate messages?

        double elapsed_time;
        boolean printme = false;        long my_total_json_messages_published = 0L;
        long my_raw_records_parsed = 0L;
        configureConsumer();
        configureProducer();

        // Subscribe to the topic.
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        consumer.subscribe(topics);

        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, byte[]> records;
                // TODO: is poll() thread safe?
                records = consumer.poll(POLL_INTERVAL);
                if (records.count() == 0) {
                    synchronized (this) {
                        if (printme) {
                            producer.flush();
                            System.out.println("========== " +
                                    Thread.currentThread().getName() +
                                    " has seen zero messages in " + POLL_INTERVAL / 1000 +
                                    "s. Raw consumed (all threads) = " +
                                    raw_records_parsed + ". JSON published (all threads) = " +
                                    json_messages_published  + " ==========");

                            System.out.println(sender_topics.size() + " sender topics:");
//                            sender_topics.forEach(t -> System.out.println("\t" + t));
                            System.out.println(receiver_topics.size() + " receiver topics:");
//                            receiver_topics.forEach(t -> System.out.println("\t" + t));
                            System.out.flush();
                            printme = false;
                        }
                    }
                } else {
                    if (!printme) {
                        // Oh! We're getting messages again. Reset metric counters.
                        synchronized (this) {
                            // Check printme flag again since we're now synchronized
                            raw_records_parsed = 0;
                            my_raw_records_parsed = 0;
                            json_messages_published = 0;
                            my_json_messages_published = 0;
                            my_total_json_messages_published = 0;
                            start_time = System.nanoTime();
                            my_last_update = 0;
                            printme = true;
                        }
                    }
                }

                for (ConsumerRecord<String, byte[]> record : records) {
                    Tick json = new Tick(record.value());
                    my_raw_records_parsed++;
                    routeToTopic(record.key(), json);
                    my_json_messages_published++;

                    // update metrics and print status once per second on each thread
                    elapsed_time = (System.nanoTime() - start_time) / 1e9;
                    if (Math.round(elapsed_time) > my_last_update) {
                        // update metrics
                        synchronized (this) {
                            raw_records_parsed += my_raw_records_parsed;
                            json_messages_published += my_json_messages_published;
                        }

                        // print status
                        System.out.printf("----- t=%.0fs. Total messages published = %d. Throughput= %.2f Kmsgs/sec -----\n",
                                elapsed_time,
                                json_messages_published,
                                json_messages_published / elapsed_time / 1000);
                        System.out.printf("\t" + Thread.currentThread().getName() + " published %d. Tput = %.2f Kmsgs/sec\n",
                                my_total_json_messages_published,
                                my_total_json_messages_published / elapsed_time / 1000);

                        my_raw_records_parsed = 0;
                        my_total_json_messages_published += my_json_messages_published;
                        my_json_messages_published = 0;
                        my_last_update = Math.round(elapsed_time);

                    }
                }
            }

        } catch (Exception e) {
            System.err.println("ERROR: " + e);
        } finally {
            consumer.close();
            System.out.println("Consumed " + raw_records_parsed + " messages from stream (all threads).");
            System.out.println("Finished.");
        }
    }

    private void routeToTopic(String key, Tick tick) {
        ProducerRecord<String, byte[]> record;

        String topic = "/user/mapr/taq:sender_" + tick.getSender();
        sender_topics.add(topic);
        publish (topic, key, tick.getData());
        // TODO: save record to maprdb

        for (String receiver : tick.getReceivers())
        {
            topic = "/user/mapr/taq:receiver_" + receiver;
            receiver_topics.add(topic);
            publish (topic, key, tick.getData());

            // TODO: save record to maprdb

        }
    }

    private void publish(String topic, String key, byte[] data) {
        // Non-blocking send. Callback invoked when request is complete.
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, data);
        producer.send(record,
                new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (metadata == null || e != null) {
                            // If there appears to have been an error, decrement our counter metric
                            my_json_messages_published--;
                            e.printStackTrace();
                        } else {
//                            OffsetTracker offset = new OffsetTracker();
//                            offset.topic = metadata.topic();
//                            offset.partition = metadata.partition();
//                            offset.offset = metadata.offset();
//                            offset.timestamp = new Tick(data).getDate();
//                            offset_cache.putIfAbsent(new Tuple<>(metadata.topic(), metadata.partition()), offset);
                        }
                    }
                });
    }
}