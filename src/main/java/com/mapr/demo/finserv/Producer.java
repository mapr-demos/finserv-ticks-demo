package com.mapr.demo.finserv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

public class Producer {

    public static KafkaProducer producer;
    static long records_processed = 0L;

    public static void main(String[] args) throws IOException {
        if (args.length < 3) {
            System.err.println("ERROR: You must specify the input data file and stream:topic.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer data/taqtrade20131218 /usr/mapr/taq:trades");

        }

        String topic =  args[2] ;
        System.out.println("Publishing to topic: "+ topic);

        configureProducer();
        System.out.println("Opening file " + args[1]);
        File f = new File(args[1]);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();


        try {
            long startTime = System.nanoTime();
            long last_update = 0;

            while (line != null) {

                long current_time = System.nanoTime();
                String key = Long.toString(current_time);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, line);

                // Send the record to the producer client library.
//                producer.send(rec);
                producer.send(record,
                        new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                records_processed++;
                                if(e != null)
                                    e.printStackTrace();
                            }
                        });


                // Print performance stats once per second
                if ((Math.floor(current_time - startTime)/1e9) > last_update)
                {
                    last_update ++;
                    producer.flush();
                    print_status(records_processed, 1, startTime);
                }
                line = reader.readLine();
            }

        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
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

    public static void print_status(long records_processed, int poolSize, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
        System.out.printf("Throughput = %.2f Kmsgs/sec published. Threads = %d. Total published = %d.\n",
                records_processed / ((double) elapsedTime / 1000000000.0) / 1000,
                poolSize,
                records_processed);
    }
}
