package com.mapr.test;

/******************************************************************************
 * PURPOSE:
 *   This Kafka consumer is designed to measure how fast we can consume
 *   messages from a topic and persist them to MapR-DB. It output throughput
 *   stats to stdout.
 *
 *   This Kafka consumer reads NYSE Tick data from a MapR Stream topic and
 *   persists each message in a MapR-DB table as a JSON Document, which can
 *   later be queried using Apache Drill (for example).
 *
 * EXAMPLE USAGE:
 *   java -cp ~/nyse-taq-streaming-1.0.jar:$CP com.mapr.demo.finserv.Persister /user/mapr/taq:sender_1361
 *
 * EXAMPLE QUERIES FOR MapR dbshell:
 *      mapr dbshell
 *          find /user/mapr/ticktable
 *
 * EXAMPLE QUERIES FOR APACHE DRILL:
 *      /opt/mapr/drill/drill-1.6.0/bin/sqlline -u jdbc:drill:
 *          SELECT * FROM dfs.`/mapr/ian.cluster.com/user/mapr/ticktable`;
 *          SELECT * FROM dfs.`/user/mapr/ticktable`;
 *
 *****************************************************************************/

import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import com.mapr.demo.finserv.Tick;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.ojai.Document;

import java.io.IOException;
import java.util.*;

public class PersisterSpeedTest {

    public static KafkaConsumer consumer;
    private static boolean PURGE = false;

    static long records_consumed = 0L;

    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);
        String tableName = "";
        Table table = null;

        String topic = args[0];
        String group_id = args[1];
        if (args.length>2) {
            tableName = args[2];
            System.out.println("Persisting to table " + tableName);
            if (args.length == 4 && "purge".equals(args[3])) PURGE=true;
            if (PURGE) {
                // delete the old table if it's there
                if (MapRDB.tableExists(tableName)) {
                    System.out.println("deleting old table " + tableName);
                    MapRDB.deleteTable(tableName);
                }
                // make a new table
                table = MapRDB.createTable(tableName);
            } else {
                table = MapRDB.getTable(tableName);
            }

            // probably want this
            table.setOption(Table.TableOption.BUFFERWRITE, false);

        }


        System.out.println("Enter to continue...");
        Scanner scanner = new Scanner(System.in);
        String user_input = scanner.nextLine();


        configureConsumer(group_id);

        List<String> topics = new ArrayList<>();
        topics.add(topic);
        System.out.println("Subscribing to " + topic);
        consumer.subscribe(topics);
        long pollTimeOut = 1000;  // milliseconds
        boolean printme = false;
        long start_time = 0;
        long last_update = 0;
        long startTime = System.nanoTime();
        Integer[] partitions = {0, 0, 0};
        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, byte[]> records = consumer.poll(pollTimeOut);
                long current_time = System.nanoTime();
                double elapsed_time = (current_time - start_time)/1e9;
                if (records.count() == 0) {
                    System.out.println("No messages after " + pollTimeOut / 1000 + "s. Total msgs consumed = " +
                            records_consumed + ". Duration =" + Math.round(elapsed_time) + "s. Average ingest rate = " + Math.round(records_consumed / elapsed_time / 1000) + "Kmsgs/s");
                }
                if (records.count() > 0) {
                    if (printme == false) {
                        start_time = current_time;
                        last_update = 0;
                        records_consumed = 0;
                        printme = true;
                    }
                    for (ConsumerRecord<String, byte[]> record : records) {
                        records_consumed++;
                        partitions[record.partition()] = 1;
                        if (tableName.length()>0) {
                            Tick tick = new Tick(record.value());
                            Document document = MapRDB.newDocument((Object)tick);
                            table.insertOrReplace(tick.getTradeSequenceNumber(), document);
                        }
                    }

                    // Print performance stats once per second
                    if ((Math.floor(current_time - start_time)/1e9) > last_update)
                    {
                        last_update ++;

                        System.out.printf("t = %d. Total msgs consumed = %d. Average ingest rate = %.3f Kmsgs/s. Partitions = %s\n",  Math.round(elapsed_time), records_consumed, records_consumed / elapsed_time / 1000, Arrays.toString(partitions));
//                        System.out.println("t = " + Math.round(elapsed_time) + ". Total msgs consumed = " + records_consumed + ". Average ingest rate = " + Math.round(records_consumed / elapsed_time / 1000) + "Kmsgs/s. Partitions = " + Arrays.toString(partitions));
                        Integer[] tmp = {0,0,0};
                        partitions = tmp;
                    }

                    consumer.commitSync();
                }

            }



        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            System.out.println("Consumed " + records_consumed + " messages.");
            System.out.println("Finished.");
            consumer.close();
        }


    }

    /* Set the value for configuration parameters.*/
    public static void configureConsumer(String group_id) {
        Properties props = new Properties();
        props.put("enable.auto.commit","false");
//        props.put("group.id", UUID.randomUUID().toString());
        props.put("group.id", group_id);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

}


