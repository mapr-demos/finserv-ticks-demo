package com.mapr.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BasicConsumerRelay {

    public static KafkaConsumer consumer;
    public static KafkaProducer producer;
    private static boolean VERBOSE = false;

    static long records_processed = 0L;

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        if (args.length < 2) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.BasicConsumerRelay stream:consume_topic stream:produce_topic [verbose]\n" +
                    "EXAMPLE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.BasicConsumerRelay /user/mapr/taq:test01  /user/mapr/taq:test02");

        }

        String topic1 = args[0];
        System.out.println("Consuming from stream: " + topic1);
        String topic2 = args[1];
        System.out.println("Producing to stream: " + topic2);

        if (args.length == 3 && "verbose".equals(args[2])) VERBOSE=true;

        configureConsumer();
        configureProducer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic1);
        // Subscribe to the topic.
        consumer.subscribe(topics);
        long pollTimeOut = 5000;  // milliseconds
        boolean printme = false;
        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (records.count() == 0) {
                    if (printme) {
                        producer.flush();
                        System.out.println("\nNo messages after " + pollTimeOut / 1000 + "s. Total msgs relayed = " +
                                records_processed);
                        printme = false;
                    }
                }
                if (records.count() > 0) {
                    printme = true;
                    for (ConsumerRecord<String, String> record : records) {
                        long current_time = System.nanoTime();

                        if (VERBOSE) {
                            System.out.printf("\tconsumed: '%s'\n" +
                                            "\t\tdelay = %.2fs\n" +
                                            "\t\ttopic = %s\n" +
                                            "\t\tpartition = %d\n" +
                                            "\t\tkey = %s\n" +
                                            "\t\toffset = %d\n",
                                    record.value(),
                                    (current_time - Long.valueOf(record.key()))/1e9,
                                    record.topic(),
                                    record.partition(),
                                    record.key(),
                                    record.offset());

                            System.out.println("\t\tRelaying to topic " + topic2);
                        }

                        String value2 = record.value();
                        String key2 = record.key();  // assumed to be timestamp of original message

                        ProducerRecord rec = new ProducerRecord(topic2,key2,value2);
                        producer.send(rec,
                                new Callback() {
                                    public void onCompletion(RecordMetadata metadata, Exception e) {
                                        long current_time = System.nanoTime();
                                        records_processed++;
                                        System.out.print(".");

                                        if (VERBOSE) {
                                            System.out.printf("\tRelayed: '%s'\n" +
                                                            "\t\tdelay = %.2f\n" +
                                                            "\t\ttopic = %s\n" +
                                                            "\t\tpartition = %d\n" +
                                                            "\t\toffset = %d\n",
                                                    value2,
                                                    (current_time - Long.valueOf(key2)) / 1e9,
                                                    metadata.topic(),
                                                    metadata.partition(), metadata.offset());
                                            System.out.println("\t\tTotal records published : " + records_processed);
                                        }
                                    }
                                });

                        if (value2.equals("q")) {
                            producer.flush();
                            System.out.println("\nRelayed " + records_processed + " messages from " + topic1 + " to " + topic2);
                            records_processed = 0;
                        }

                        consumer.commitSync();
                    }

                }

            }

        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            consumer.close();
            producer.flush();
            producer.close();
            System.out.println("Relayed " + records_processed + " messages.");
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

    /* Set the value for a configuration parameter.
 This configuration parameter specifies which class
 to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }
}


