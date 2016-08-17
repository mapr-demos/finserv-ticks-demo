package com.mapr.demo.finserv;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by idownard on 8/16/16.
 */
public class TopicRouter implements Runnable  {
    private static final int PERIOD = 1000;
    public static KafkaProducer producer;
    private long count = 0;
    static ConcurrentHashMap<Tuple, OffsetTracker> offset_cache = new ConcurrentHashMap<>();

    @Override
    public void run() {
        //configure producer
        configureProducer();

        while (true) {
            ProducerRecord<String, byte[]> record = Consumer.unrouted_messages.poll();
            if (record != null) {
                producer.send(record,
                        new Callback() {
                            public void onCompletion(RecordMetadata metadata, Exception e) {
                                if (metadata == null || e != null) {
                                    e.printStackTrace();
                                } else {
                                    count ++;
                                    Tuple key = new Tuple<>(metadata.topic(), metadata.partition());
                                    if (!offset_cache.containsKey(key)) {
                                        OffsetTracker offset = new OffsetTracker();
                                        offset.topic = metadata.topic();
                                        offset.partition = metadata.partition();
                                        offset.offset = metadata.offset();
                                        offset.timestamp = new Tick(record.value()).getDate();
                                        offset_cache.put(key, offset);
                                    }
                                }
                            }
                        });
            } else {
                try {
                    System.out.println("Routed " + count + " messages to sender/receiver topics, so far.");
                    Thread.sleep(PERIOD);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.ByteArraySerializer");

        producer = new KafkaProducer<String, String>(props);
    }

}
