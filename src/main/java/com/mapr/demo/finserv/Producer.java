package com.mapr.demo.finserv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.Properties;

public class Producer {

    private static KafkaProducer producer;
    private static LinkedList<File> datafiles = new LinkedList<>();
    static long records_processed = 0L;


    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("ERROR: You must specify the input data file and stream:topic.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source file | source directory] [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer data/taqtrade20131218 /usr/mapr/taq:trades");

        }

        String topic = args[2];
        System.out.println("Publishing to topic: " + topic);

        configureProducer();
        File directory = new File(args[1]);

        if (directory.isDirectory()) {
            for (final File fileEntry : directory.listFiles()) {
                if (fileEntry.isDirectory()) {
                    System.err.println("WARNING: skipping files in directory " + fileEntry.getName());
                } else {
                    datafiles.add(fileEntry);
                }
            }
        } else {
            datafiles.add(directory);
        }

        System.out.println ("Publishing data from " + datafiles.size() + " files.");
        long startTime = System.nanoTime();
        long last_update = 0;

        for (final File f : datafiles) {
            FileReader fr = new FileReader(f);
            BufferedReader reader = new BufferedReader(fr);
            String line = reader.readLine();

            try {
                while (line != null) {
                    long current_time = System.nanoTime();
                    String key = Long.toString(current_time);
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, line.getBytes(Charsets.ISO_8859_1));

                    // Send the record to the producer client library.
//                producer.send(rec);
                    producer.send(record,
                            new Callback() {
                                public void onCompletion(RecordMetadata metadata, Exception e) {
                                    records_processed++;
                                    if (e != null)
                                        e.printStackTrace();
                                }
                            });


                    // Print performance stats once per second
                    if ((Math.floor(current_time - startTime) / 1e9) > last_update) {
                        last_update++;
                        producer.flush();
                        print_status(records_processed, 1, startTime);
                    }
                    line = reader.readLine();
                }

            } catch (Exception e) {
                System.err.println("ERROR: " + e);
            }
        }
        producer.flush();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Published " + records_processed + " messages to stream.");
        System.out.println("Finished.");
        producer.close();
    }

    /* Set the value for a configuration parameter.
     This configuration parameter specifies which class
     to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<String, String>(props);
    }

    public static void print_status(long records_processed, int poolSize, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        System.out.printf("Throughput = %.2f Kmsgs/sec published. Threads = %d. Total published = %d.\n",
                records_processed / ((double) elapsedTime / 1000000000.0) / 1000,
                poolSize,
                records_processed);
    }
}
