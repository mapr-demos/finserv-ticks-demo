package com.mapr.demo.finserv;

/******************************************************************************
 * PURPOSE:
 * This class is intended to allow one to query the NYSE TAQ data
 * in a Kafka stream, and ask the question, "Show me all the trades involving
 * participant A in the past X minutes", where "A" is the ID for a seller or receiver.
 *
 * EXAMPLE USAGE:
 * /opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkQuerier /mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1142
 *
 * AUTHOR:
 * Ian Downard, idownard@mapr.com
 *****************************************************************************/

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.hive.*;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.v09.KafkaUtils;

import java.util.*;

public class SparkQuerier {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkQuerier target/nyse-taq-streaming-1.0-jar-with-dependencies.jar stream:topic\n" +
                    "EXAMPLE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.demo.finserv.SparkQuerier /mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:sender_1142");
        }

        SparkConf conf = new SparkConf()
                .setAppName("Spark Streaming")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc.sc());

        String topic = args[0];
        Set<String> topics = Collections.singleton(topic);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");

        // Connect to Kafka topic
        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, kafkaParams, topics);

        System.out.println("Waiting for messages on topic " + topic);
        // Build an RDD with the fields we want to query
        directKafkaStream.foreachRDD(rdd -> {

            // Generate the schema for the dataframe that we will be constructing
            List<StructField> fields = new ArrayList<StructField>();
            fields.add(0,DataTypes.createStructField("date", DataTypes.StringType, true));
            fields.add(1,DataTypes.createStructField("symbol", DataTypes.StringType, true));
            fields.add(2,DataTypes.createStructField("price", DataTypes.DoubleType, true));
            fields.add(3,DataTypes.createStructField("volume", DataTypes.DoubleType, true));
            StructType schema = DataTypes.createStructType(fields);

            if (rdd.count() > 0) {
                System.out.println("reading messages...");
                // save the raw data string in each row
                JavaRDD<String> tick = rdd.map(x -> new String(x._2));
                // Parse each row using the helper methods in the Tick data structure
                JavaRDD<Row> rowRDD = tick.map(
                        new Function<String, Row>() {
                            public Row call(String record) throws Exception {
                                if (record.trim().length() > 0) {
                                    Tick t = new Tick(record.trim());
                                    return RowFactory.create(t.getDate(), t.getSymbolRoot(), t.getTradeVolume(), t.getTradePrice());
                                } else {
                                    return RowFactory.create("", "", "", "");
                                }
                            }
                        });

                // Apply our user-defined schema to the RDD of parsed data
                DataFrame tick_table = sqlContext.createDataFrame(rowRDD, schema);
                // Create another dataframe to construct a Hive table and share with Zeppelin
                DataFrame tick_table_for_zeppelin = hiveContext.createDataFrame(rowRDD, schema);

                // Register the DataFrame as a table so we can query it with SQL
                tick_table.registerTempTable("temptick");


                tick_table_for_zeppelin.printSchema();
                tick_table_for_zeppelin.show(5);
                tick_table_for_zeppelin.saveAsTable("tick", SaveMode.Append);


                try {
                    // Run a SQL query over the RDD that we registered as table.
                    DataFrame sql_result = sqlContext.sql("SELECT symbol FROM temptick");
                    JavaRDD javardd = sql_result.javaRDD();
                    List<String> elements = javardd.map((Function<Row, String>) row ->
                            row.getString(0)).collect();
                    //elements.forEach(item -> System.out.println("Symbol: " + item));
                    System.out.println("Loaded " + elements.size() + " rows from SQL query.");
                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }
            }
        });

        // TODO: test getting offsets from another topic, to use for fetching a subset of a topic

        ssc.start();
        ssc.awaitTermination();
    }
}