package com.mapr.demo.finserv;


import com.google.common.base.Charsets;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.TreeSet;

/**
 * Created by idownard on 8/16/16.
 */
public class FiveMinuteTimer implements Runnable  {
//    private static final int PERIOD = 300*1000;  // 5 minutes
    private static final int PERIOD = 10000;  // TODO: change this to 5 minutes
    public static KafkaProducer producer;
    private int count = 0;
    private static TreeSet<String> offset_topics = new TreeSet<>();

    @Override
    public void run() {
        //configure producer
        configureProducer();

        // TODO: every 5 minutes, save offset and timestamp for each topic
        while (true) {
            try {
                Thread.sleep(PERIOD);
                if (TopicRouter.offset_cache != null) {
                    for (Tuple key : TopicRouter.offset_cache.keySet()) {
                        OffsetTracker offset = TopicRouter.offset_cache.get(key);
                        String topic = offset.topic+"-offset";

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HHmmssSSS");
                        LocalTime timestamp = LocalTime.parse(offset.timestamp, formatter);

                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, offset.partition, timestamp.toString(), offset.offset.toString());
                        producer.send(record,
                            new Callback() {
                                public void onCompletion(RecordMetadata metadata, Exception e) {
                                    if (metadata == null || e != null) {
                                        e.printStackTrace();
                                    }
                                    else {
                                        TopicRouter.offset_cache.remove(key);
                                        count ++;
                                        offset_topics.add(topic);
//                                        System.out.println(topic + " " + timestamp.toString() + " " + offset.offset);
                                    }
                                }
                            });
                    }
                }
                System.out.println("\tRecorded " + count + " offsets for " + offset_topics.size() + " topics, so far.");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

}
