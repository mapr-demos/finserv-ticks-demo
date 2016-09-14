package com.mapr.demo.finserv;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.kafka.v09.OffsetRange;
import java.io.IOException;
import com.mapr.demo.finserv.Tick;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.HasOffsetRanges;
import org.apache.spark.streaming.kafka.v09.KafkaTestUtils;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.apache.spark.streaming.kafka.v09.OffsetRange;
import scala.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Scanner;

public class SparkStreamingConsole {

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

    private static final int NUM_THREADS = 1;
    private static final int BATCH_INTERVAL = 5000;
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkStreamingConsole /mapr/ian.cluster.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1361 [fromOffset]\n");
        }

        long latestOffset;
        long fromOffset=0;
            Scanner user_input = new Scanner(System.in);
            SparkConf conf = new SparkConf()
                    .setAppName("TAQ Spark Streaming")
                    .setMaster("local[" + NUM_THREADS + "]")
                    .set("spark.driver.allowMultipleContexts", "true");
            JavaSparkContext sc = new JavaSparkContext(conf);
            JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(BATCH_INTERVAL));
            HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());

            String topic = args[0];

            if (args.length == 2)
                fromOffset = Long.parseLong(args[1]);

            configureConsumer();
            System.out.println("subscribing to topic: " + topic);

            consumer.subscribe(Arrays.asList(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(200);
            System.out.println(records.count() + " records available.");

            latestOffset = getLatestOffset(topic, 0);
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            DateFormat formatter = new SimpleDateFormat("HH:mm:ss:ms MM/dd/yyyy");
            System.out.println("Latest offset = " + latestOffset + " at time " + formatter.format(cal.getTime()));
            System.out.println("Press ENTER to see refresh.");
            if (fromOffset == 0) {
                System.out.print("Enter desired fromOffset: ");
                String input = user_input.nextLine();
                if (input.length() == 0)
                    continue;
                fromOffset = Long.parseLong(input);
            }

            System.out.println("latest offset: " + latestOffset + " desired fromOffset: " + fromOffset);

            // see how far we need to look back in the stream
            // this is used to specify the range of offsets we want in the RDD,
            // which will be fetched from Kafka/Streams
            OffsetRange[] offsetRanges = {
                    OffsetRange.create(topic, 0, fromOffset, latestOffset)
            };

            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

            JavaRDD<String> rdd = KafkaUtils.createRDD(
                    sc,
                    String.class,
                    byte[].class,
                    kafkaParams,
                    offsetRanges
            ).map(
                    new Function<Tuple2<String, byte[]>, String>() {
                        @Override
                        public String call(scala.Tuple2<String, byte[]> record) throws Exception {
                            byte[] data = record._2;
                            Tick t = new Tick(data);
                            StringBuilder receivers = new StringBuilder();
                            for (String id : t.getReceivers())
                                receivers.append(id + " ");
                            System.out.printf("%s, %s, %s, %s, %.0f, %.2f\n", t.getDate(), t.getSender(), receivers, t.getSymbolRoot(), t.getTradeVolume(), t.getTradePrice());
                            return new String(record._2());
                        }
                    }
            );

            System.out.println("--------------------------------\nrdd.count = " + rdd.count());
            fromOffset = 0;
        }

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