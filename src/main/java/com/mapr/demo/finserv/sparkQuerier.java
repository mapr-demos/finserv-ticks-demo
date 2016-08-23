package com.mapr.demo.finserv;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka.v09.OffsetRange;
import java.io.IOException;
import java.util.*;

public class sparkQuerier {

    private static KafkaConsumer consumer;

    // get the desired offset to look back N seconds
    private static long getOffset(Integer secs) {
        //
        // in the future version of this function we will:
        // - query MapR-DB or a Streams topic to get the expected offset in the stream
        // - return the offset, which will be used as an offsetRange when setting up the context
        //
        // for now, we just do a simple map of minutes -> offset
        // this could be way off but just for example purposes
        //
        final long OFFSETS_PER_SEC = 300000;

        return (secs * OFFSETS_PER_SEC);
    }

    // get the latest offset in a topic+partition
    private static long getLatestOffset(String topic, int partition) {
        long pos;

        TopicPartition tp = new TopicPartition(topic, partition);

        // seek to the current end of the topic
        consumer.seekToEnd(tp);

        // get the offset of where that is
        pos = consumer.position(tp);

        return (pos);
    }

    public static void main(String[] args) {
        long latest, desired;

        if (args.length != 2) {
            System.out.println("Usage:  <cmd> <sender_id> <time in seconds>");
            System.exit(-1);
        }
        String topic = args[0];
        String lookback = args[1];

        configureConsumer();
        System.out.println("subscribing to topic: " + topic);

        consumer.subscribe(Arrays.asList(topic));

        // read records with a short timeout. If we time out, we don't really care.
        // looks like because of KAFKA-2359
        // we have to do a poll() before any partitions are assigned?
        // else we get an error that we are doing a seek() on an unsubscribed partition
        // the records from poll() also have to be evaluated, the below println seems to do it
        ConsumerRecords<String, String> records = consumer.poll(200);
        System.out.println("got " + records.count() + " records");

        latest = getLatestOffset(topic, 0);
        desired = latest - getOffset(Integer.parseInt(lookback));

        System.out.println("latest offset: " + latest + " desired offset: " + desired);

        // see how far we need to look back in the stream
        // this is used to specify the range of offsets we want in the RDD,
        // which will be fetched from Kafka/Streams
        OffsetRange[] offsetRanges = {
                OffsetRange.create(topic, 0, desired, latest)
        };
        System.exit(-1);

//        JavaRDD<String> rdd1 = KafkaUtils.createRDD(
//                sc,
//                String.class,
//                String.class,
//                StringDecoder.class,
//                StringDecoder.class,
//                kafkaParams,
//                offsetRanges
//        ).map(
//                new Function<Tuple2<String, String>, String>() {
//                    @Override
//                    public String call(scala.Tuple2<String, String> kv) throws Exception {
//                        return kv._2();
//                    }
//                }
//        );
    }

    /* Set the value for configuration parameters.*/
    private static void configureConsumer() {
        Properties props = new Properties();
        props.put("group.id","group-" + new Random().nextInt(100000));
        props.put("enable.auto.commit","true");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("auto.offset.reset","latest");

        consumer = new KafkaConsumer<String, String>(props);
    }
}
