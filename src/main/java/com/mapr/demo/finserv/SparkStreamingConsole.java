package com.mapr.demo.finserv;

import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.kafka.v09.OffsetRange;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import scala.Tuple2;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Scanner;

public class SparkStreamingConsole {

    private static KafkaConsumer offset_consumer;
    private static KafkaConsumer consumer;
    private static final DateFormat FORMATTER = new SimpleDateFormat("HH:mm:ss:SSS z");

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
    private static long getLatestOffset(KafkaConsumer c, String topic, int partition) {
        long pos;

        TopicPartition tp = new TopicPartition(topic, partition);

        // seek to the current end of the topic
        c.seekToEnd(tp);

        // get the offset of where that is
        pos = c.position(tp);

        return (pos);
    }

    private static final int NUM_THREADS = 2;
    private static final int BATCH_INTERVAL = 5000;
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingConsole /mapr/ian.cluster.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1361\n");
            System.exit(1);
        }

        long latestOffset;
        long fromOffset;

        SparkConf conf = new SparkConf()
                .setAppName("TAQ Spark Streaming")
                .setMaster("local[" + NUM_THREADS + "]")
                .set("spark.driver.allowMultipleContexts", "true");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String topic = args[0];
        String offset_topic = topic+"-offset";

        Scanner scanner = new Scanner(System.in);

        configureConsumer();
        System.out.println("subscribing to topic: " + offset_topic);
        offset_consumer.subscribe(Arrays.asList(offset_topic));
        System.out.println("subscribing to topic: " + topic);
        consumer.subscribe(Arrays.asList(topic));

        System.out.println("--------------------------------------------");
        // determine fromOffset for lookback
        Boolean quit = false;
        while (!quit) {
            Boolean found = false;
            System.out.println("How many seconds do you want to records for?");
            System.out.println("Enter q to quit.");
            String user_input = scanner.nextLine();
            if (user_input.equals("q")){
                quit=true;
                continue;
            }
            if (user_input.length() == 0) continue;
            if (Long.parseLong(user_input) < 0) {
                System.out.println("Input a number larger than 0.");
                continue;
            }
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            long from_time = cal.getTimeInMillis() - Long.parseLong(user_input)*1000;    // past time (in seconds) for which to fetch records
            cal.setTimeInMillis(from_time);
            System.out.println("Fetching records posted to topic " + topic + " since time " + FORMATTER.format(cal.getTime()));

            try {
                while (!found) {
                    offset_consumer.poll(2000);

                    latestOffset = getLatestOffset(offset_consumer, offset_topic, 0);

                    OffsetRange[] offsetRanges = {
                            OffsetRange.create(topic + "-offset", 0, 0, latestOffset)
                    };

                    Map<String, String> kafkaParams = new HashMap<>();
                    kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

//                    List<Tuple2<Long, Long>> timed_offsets = KafkaUtils.createRDD(
//                            sc,
//                            String.class,
//                            String.class,
//                            kafkaParams,
//                            offsetRanges
//                    ).map(record -> new Tuple2<Long, Long>(Long.parseLong(record._1), Long.parseLong(record._2))
//                    ).collect();

                    JavaPairRDD<String, String> rdd = KafkaUtils.createRDD(
                            sc,
                            String.class,
                            String.class,
                            kafkaParams,
                            offsetRanges
                    );

                    List<Tuple2<Long, Long>> timed_offsets = rdd.map(
                            new Function<Tuple2<String,String>, Tuple2<Long, Long>>() {
                                public Tuple2<Long, Long> call(Tuple2<String,String> record) {
                                    return new Tuple2<Long, Long>(Long.parseLong(record._1), Long.parseLong(record._2));
                                }
                            }).collect();

                    for (int i = 0; i < timed_offsets.size() && !found; i++) {
                        Long timestamp = timed_offsets.get(i)._1;
                        if (timestamp >= from_time) {
                            fromOffset = timed_offsets.get(i)._2;
                            found = true;
                            cal.setTimeInMillis(timed_offsets.get(i)._1);
                            System.out.println("offset " + fromOffset + " corresponds to time " + FORMATTER.format(cal.getTime()));
                            System.out.println("Using offset " + fromOffset);
                            read_from_offset(sc, topic, fromOffset, user_input);
                        }
                    }
                    if (!found) {
                        System.out.println("No records found in that time range.");
                        found = true;
                    }
                }
            } catch (org.apache.kafka.common.errors.UnknownTopicOrPartitionException e) {
                System.out.println ("Topic " + offset_topic + "does not exist, yet.");
            }
        }
        consumer.unsubscribe();
        offset_consumer.unsubscribe();
    }

    private static void read_from_offset(JavaSparkContext sc, String topic, long fromOffset, String input) {
        consumer.poll(2000);
        System.out.println("getting latestOffset for topic: " + topic);
        long latestOffset = getLatestOffset(consumer, topic, 0);
        System.out.println("fetching fromOffset=" + fromOffset + " untilOffset=" + latestOffset);

        OffsetRange[] offsetRanges = {
                OffsetRange.create(topic, 0, fromOffset, latestOffset)
        };

        Map<String, String> kafkaParams2 = new HashMap<>();
        kafkaParams2.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams2.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        JavaRDD<String> rdd = KafkaUtils.createRDD(
                sc,
                String.class,
                byte[].class,
                kafkaParams2,
                offsetRanges
        ).map(
                new Function<Tuple2<String, byte[]>, String>() {
                    @Override
                    public String call(Tuple2<String, byte[]> record) throws Exception {
                        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                        String key = record._1;
                        byte[] value = record._2;
                        cal.setTimeInMillis(Long.parseLong(key));
                        System.out.println("timesstamp=" + FORMATTER.format(cal.getTime()));
                        // output Tick in JSON format
                        System.out.println("\t"+new ObjectMapper().writeValueAsString(new Tick(value)));
                        return new String(value);
                    }
                }
        );
        System.out.println("--------------------------------\n" + rdd.count() + " trades recorded in " + topic + " over the past " + input + " seconds.");
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

        offset_consumer = new KafkaConsumer<String, String>(props);
        consumer = new KafkaConsumer<String, byte[]>(props);
    }
}