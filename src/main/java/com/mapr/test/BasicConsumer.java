package com.mapr.test;

import java.io.*;

import com.mapr.demo.finserv.Monitor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BasicConsumer {

    public static KafkaConsumer consumer;
    static long records_processed = 0L;
    private static boolean VERBOSE = false;

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.BasicConsumer stream:topic [verbose]\n" +
                    "EXAMPLE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.BasicConsumerRelay /user/mapr/taq:test02");

        }

        String topic = args[0];
        System.out.println("Consuming from stream: " + topic);

        if (args.length == 2 && "verbose".equals(args[1])) VERBOSE=true;

        configureConsumer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);
        long pollTimeOut = 1000;  // milliseconds

        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (records.count() > 0) {
                    for (ConsumerRecord<String, String> record : records) {
                        records_processed++;
                        long current_time = System.nanoTime();
                        int i = record.value().indexOf(' ');
                        System.out.print(".");

                        if (VERBOSE) {
                            System.out.printf("\tconsumed: '%s'\n" +
                                            "\t\tdelay = %.2fs\n" +
                                            "\t\ttopic = %s\n" +
                                            "\t\tpartition = %d\n" +
                                            "\t\tkey = %s\n" +
                                            "\t\toffset = %d\n",
                                    record.value().substring(i+1),
                                    (current_time - Long.valueOf(record.value().substring(0,i)))/1e9,
                                    record.topic(),
                                    record.partition(),
                                    record.key(),
                                    record.offset());
                            System.out.println("\t\tTotal records consumed : " + records_processed);
                        }

                        if (record.value().substring(i+1).equals("q")) {
                            System.out.println("\nConsumed " + records_processed + " messages from stream.");
//                            System.out.println("Exit!");
//                            consumer.commitSync();
//                            System.exit(0);
                        }
                        consumer.commitSync();
                    }

                }

            }

        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
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
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }
}
