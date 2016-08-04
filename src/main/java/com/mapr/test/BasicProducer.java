package com.mapr.test;

import java.io.*;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;

public class BasicProducer {

    public static KafkaProducer producer;
    static long records_processed = 0L;
    private static boolean VERBOSE = false;


    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.BasicProducer stream:topic [verbose]\n" +
                    "EXAMPLE:\n" +
                    "\techo `seq 1 1000` q | tr ' ' '\\n' | java -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.test.BasicProducer /user/mapr/taq:test01");

        }

        String topic = args[0];
        System.out.println("Publishing to stream: " + topic);

        if (args.length == 2 && "verbose".equals(args[1])) VERBOSE=true;

        configureProducer();

        BufferedReader br = null;

        try {


            br = new BufferedReader(new InputStreamReader(System.in));

            System.out.println("Enter 'q' to quit. ");
            System.out.println("Ready for input... ");

            while (true) {

                String value = br.readLine();
                if (value == null) break;

                String key = Long.toString(System.nanoTime());
                ProducerRecord rec = new ProducerRecord(topic,key,value);
                producer.send(rec,
                        new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                long current_time = System.nanoTime();
                                records_processed++;
                                System.out.print(".");

                                if (VERBOSE) {
                                    System.out.printf("\tSent: '%s'\n" +
                                                    "\t\tdelay = %.2f\n" +
                                                    "\t\ttopic = %s\n" +
                                                    "\t\tpartition = %d\n" +
                                                    "\t\toffset = %d\n",
                                            value,
                                            (current_time - Long.valueOf(key))/1e9,
                                            metadata.topic(),
                                            metadata.partition(), metadata.offset());
                                    System.out.println("\t\tTotal records published : " + records_processed);
                                }
                            }
                        });

                if ("q".equals(value)) {
                    System.out.println("Exit!");
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
            System.out.println("\nTotal records published : " + records_processed);
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


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
