package com.mapr.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class TopicPrinter {

    public static KafkaConsumer consumer;
    static long records_processed = 0L;
    private static boolean VERBOSE = false;

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.TopicPrinter stream:topic1 [stream:topic_n] [verbose]\n" +
                    "EXAMPLE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.TopicPrinter /user/mapr/taq:test01 /user/mapr/taq:test02 /user/mapr/taq:test03 verbose");
        }

        List<String> topics = new ArrayList<String>();
        System.out.println("Consuming from streams:");
        for (int i=0; i<args.length-1; i++) {
            String topic = args[i];
            topics.add(topic);
            System.out.println("\t" + topic);
        }

        if ("verbose".equals(args[args.length-1])) VERBOSE=true;
        else {
            String topic = args[args.length-1];
            topics.add(topic);
            System.out.println("\t" + topic);
        }

        configureConsumer();

        consumer.subscribe(topics);
        long pollTimeOut = 5000;  // milliseconds
        boolean printme = false;
        long start_time = 0;
        long last_update = 0;
        double latency_total = 0;
        System.out.println("Waiting for messages...");
        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeOut);
                long current_time = System.nanoTime();
                double elapsed_time = (current_time - start_time)/1e9;
                if (records.count() == 0) {
                    if (printme) {
                        System.out.println("===== No messages after " + pollTimeOut / 1000 + "s =====");
                        System.out.printf("Total msgs consumed = %d over %ds. Avg ingest rate = %dKmsgs/s\n",
                                records_processed,
                                Math.round(elapsed_time),
                                Math.round(records_processed / elapsed_time / 1000));
                        printme = false;
                    }
                }
                if (records.count() > 0) {
                    if (printme == false) {
                        start_time = current_time;
                        last_update = 0;
                        latency_total = 0;
                        records_processed = 0;
                        printme = true;
                    }
                    for (ConsumerRecord<String, byte[]> record : records) {
                        records_processed++;
                        // NOTE: latency will not be accurate if the consumer is not running on the same server as the producer
                        if (record.key() != null)
                            latency_total = latency_total + (current_time - Long.valueOf(record.key()))/1e9;
                        if ((Math.floor(current_time - start_time)/1e9) > last_update)
                        {
                            last_update ++;
                            System.out.println("----------------------------------");
                            System.out.printf("Total msgs consumed = %d over %ds. Avg ingest rate = %dKmsgs/s\n",
                                    records_processed,
                                    Math.round(elapsed_time),
                                    Math.round(records_processed / elapsed_time / 1000));
                        }
                        if (record.key() != null) {
                            if (VERBOSE) {
                                System.out.printf("\trecord.value = '%s'\n" +
                                                "\trecord.topic = %s\n" +
                                                "\trecord.partition = %d\n" +
                                                "\trecord.key = %s\n" +
                                                "\trecord.offset = %d\n",
                                        new String(record.value()),
                                        record.topic(),
                                        record.partition(),
                                        record.key(),
                                        record.offset());
                                System.out.println("\t\tTotal records consumed = " + records_processed);
                                System.out.println("\t\tElapsed time = " + elapsed_time);
                                System.out.println("\t\tSystem.nanoTime = " + System.nanoTime());

                            }
                        }

                        if (record.value().equals("q")) {
                            System.out.println("\nConsumed " + records_processed + " messages from stream.");
                            records_processed = 0;
                        }
                        consumer.commitSync();
                    }

                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
            System.out.println("\nConsumed " + records_processed + " messages from stream.");
            System.out.println("Finished.");
        }


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
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }
}
