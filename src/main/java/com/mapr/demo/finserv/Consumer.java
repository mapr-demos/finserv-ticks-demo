package com.mapr.demo.finserv;/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Consumer {
    private static final long POLL_INTERVAL = 15000;  // consumer waits X milliseconds until thinking there is no more data
    private static final long OFFSET_INTERVAL = 10000;  // record offset once every X messages

    private static int threadCount = 1;
    static boolean VERBOSE = false;
    static private long count = 0;
    private KafkaConsumer consumer;
    private int batchSize = 0;

    private static final ProducerRecord<String, byte[]> end = new ProducerRecord<>("end", null);

    private static class Sender extends Thread {
        private final KafkaProducer<String, byte[]> producer;
        private final KafkaProducer<String, String> offset_producer;
        private final BlockingQueue<ProducerRecord<String, byte[]>> queue;

        public Sender(KafkaProducer<String, byte[]> producer, KafkaProducer<String, String> offset_producer, BlockingQueue<ProducerRecord<String, byte[]>> queue) {
            this.producer = producer;
            this.offset_producer = offset_producer;
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                ProducerRecord<String, byte[]> rec = queue.take();
                while (rec != end) {
                    final ProducerRecord<String, byte[]> rec_backup = rec;  // if send fails, add this back to the queue
                    count++;
                    // Record an offset every once in a while
                    if (count % OFFSET_INTERVAL != 0) {
                        producer.send(rec,
                                new Callback() {
                                    public void onCompletion(RecordMetadata metadata, Exception e) {
                                        if (metadata == null || e != null) {
                                            // If there appears to have been an error, decrement our counter metric
                                            count--;
                                            queue.add(rec_backup);
                                            e.printStackTrace();
                                        }
                                    }
                                }
                        );
                    } else {
                        String event_timestamp = new Tick(rec.value()).getDate();
                        producer.send(rec,
                                new Callback() {
                                    public void onCompletion(RecordMetadata metadata, Exception e) {
                                        if (metadata == null || e != null) {
                                            // If there appears to have been an error, decrement our counter metric
                                            count--;
                                            queue.add(rec_backup);
                                            e.printStackTrace();
                                        } else {
                                            offset_producer.send(new ProducerRecord<String, String>(metadata.topic() + "-offset", event_timestamp, metadata.offset() + "," + metadata.partition()));

                                        }
                                    }
                                }
                        );
                    }
                    rec = queue.take();
                }
            } catch (InterruptedException e) {
                System.out.printf("%s: Interrupted\n", this.getName());
            }
        }
    }
    long newcount=0;
    long oldcount=0;
    public void main(String[] args) {
        //Thread.sleep(5000);  // give me a change to attach a remote debugger

        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer2 [stream:topic] [NUM_THREADS] [verbose]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer2 /usr/mapr/taq:trades 2 verbose");

        }

        String topic = args[1];
        System.out.println("Subscribed to : " + topic);
        if ("verbose".equals(args[args.length-1])) VERBOSE=true;
        if (args.length == 4)
            threadCount = Integer.valueOf(args[2]);
        if (args.length == 3 && !"verbose".equals(args[2]))
            threadCount = Integer.valueOf(args[2]);

        System.out.println("Spawning " + threadCount + " consumer threads");

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        List<BlockingQueue<ProducerRecord<String, byte[]>>> queues = Lists.newArrayList();
        for (int i = 0; i < threadCount; i++) {
            BlockingQueue<ProducerRecord<String, byte[]>> q = new ArrayBlockingQueue<>(1000);
            queues.add(q);
            try {
                pool.submit(new Sender(getProducer(), getOffsetProducer(), q));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        configureConsumer();
        List<String> topics = new ArrayList<>();
        topics.add(topic);
        consumer.subscribe(topics);

        int i = 0;
        double t0 = System.nanoTime() * 1e-9;
        double t = t0;
        try {
            while (true) {
                i++;
                //int qid = i % threadCount;      // Round-robin approach, not recommended because of batching.
                int qid = 0;
                // Request unread messages from the topic.
                ConsumerRecords<String, byte[]> records;
                records = consumer.poll(POLL_INTERVAL);
                if (records == null || records.count() == 0) {
                    if (count >= 10) {
                        for (int j = 0; j < threadCount; j++)
                            queues.get(j).put(end);
                        pool.shutdown();
                        pool.awaitTermination(900, TimeUnit.SECONDS);
                        break;
                    }
                    continue;
                }

                for (ConsumerRecord<String, byte[]> raw_record : records) {
                    try {
                        String key = raw_record.key();  // We're using the key to calculate delay from when the message was sent
                        byte[] data = raw_record.value();
                        String sender_id = new String(data, 71, 4);
                        String send_topic = "/user/mapr/taq:sender_" + sender_id;
                        qid = send_topic.hashCode() % threadCount;
                        if (qid < 0) {
                            qid += threadCount;
                        }
                        queues.get(qid).put(new ProducerRecord<>(send_topic, key, data));
                        for (int j = 0; (79 + j * 4) <= data.length; j++) {
                            String receiver_id = new String(data, 75 + j * 4, 4);
                            String recv_topic = "/user/mapr/taq:receiver_" + receiver_id;
                            qid = recv_topic.hashCode() % threadCount;
                            if (qid < 0) {
                                qid += threadCount;
                            }
                            queues.get(qid).put(new ProducerRecord<>(recv_topic, key, data));
                        }
                    } catch (StringIndexOutOfBoundsException e) {
                        System.err.println("ERROR: Unexpected format for record: " + new String(raw_record.value(), 0, raw_record.value().length-1));
                        e.printStackTrace();
                    }
                }
                double dt = System.nanoTime() * 1e-9 - t;

                if (dt > 1) {
                    newcount = count - oldcount;
                    System.out.printf("Total sent: %d, %.02f Kmsgs/sec\n", count, newcount / (System.nanoTime() * 1e-9 - t0) / 1000);
                    t = System.nanoTime() * 1e-9;
                    oldcount = newcount;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    KafkaProducer<String, byte[]> getProducer() throws IOException {
        Properties p = new Properties();
        p.load(Resources.getResource("producer.props").openStream());

        if (batchSize > 0) {
            p.setProperty("batch.size", String.valueOf(batchSize));
        }
        return new KafkaProducer<>(p);
    }

    KafkaProducer<String, String> getOffsetProducer() throws IOException {
        Properties p = new Properties();
        p.load(Resources.getResource("producer.props").openStream());

        if (batchSize > 0) {
            p.setProperty("batch.size", String.valueOf(batchSize));
        }
        return new KafkaProducer<>(p);
    }

    /* Set the value for configuration parameters.*/
    private void configureConsumer() {
        Properties props = new Properties();
        props.put("group.id","group-" + new Random().nextInt(100000));
        props.put("enable.auto.commit","true");
        props.put("group.id", "mapr-workshop");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

//        props.put("fetch.min.bytes","50000");
//        props.put("max.partition.fetch.bytes","2097152");
//        props.put("auto.offset.reset","earliest");

        consumer = new KafkaConsumer<String, String>(props);
    }
}