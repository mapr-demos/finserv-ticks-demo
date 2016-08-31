package com.mapr.test;

import org.apache.spark.api.java.function.Function;
// Import factory methods provided by DataTypes.
import org.apache.spark.sql.types.DataTypes;
// Import StructType and StructField
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
// Import Row.
import org.apache.spark.sql.Row;
// Import RowFactory.
import org.apache.spark.sql.RowFactory;

import com.google.common.base.Charsets;
import com.mapr.demo.finserv.Tick;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.SparkContext;
//import org.apache.spark.sql.SparkSession;

import org.apache.spark.streaming.kafka.v09.KafkaUtils;
import org.apache.spark.unsafe.types.ByteArray;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class BasicSparkTickConsumer {
    private static PrintWriter datafile;
    private static void openDataFile() throws FileNotFoundException {
        datafile = new PrintWriter(new File("tick.csv"));
        datafile.printf("time of trade, security symbol, trade volume, trade price\n");
    }
    private static void closeDataFile() {
        datafile.close();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("ERROR: You must specify the stream:topic.");
            System.err.println("USAGE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.test.BasicSparkStringConsumer target/nyse-taq-streaming-1.0-jar-with-dependencies.jar stream:topic\n" +
                    "EXAMPLE:\n" +
                    "\t/opt/mapr/spark/spark-1.6.1/bin/spark-submit --class com.mapr.test.BasicSparkStringConsumer /mapr/tmclust1/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar /user/mapr/taq:mytopic");
        }

        try {
            openDataFile();  // for writting raw data
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        SparkConf conf = new SparkConf()
                .setAppName("Spark String Consumer")
                .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
//        SparkSession spark = SparkSession.builder().getOrCreate();

        String topic = args[0];
        Set<String> topics = Collections.singleton(topic);
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");

        JavaPairInputDStream<String, byte[]> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, byte[].class, kafkaParams, topics);

        // The schema is encoded in a string
        String _schemaString = "date symbol volume price";

        directKafkaStream.foreachRDD(rdd -> {
//            Dataset<Row> df;
            System.out.println("--- New RDD with " + rdd.partitions().size()
                    + " partitions and " + rdd.count() + " records");
//            if(rdd.count() > 0) {
//                DataFrame df = sqlContext.createDataFrame(rdd.values(), Tick.class);
//                System.out.println("dataframe count = " + df.count());
//            }

            // Generate the schema based on the string of schema
            List<StructField> _fields = new ArrayList<StructField>();
            _fields.add(0,DataTypes.createStructField("date", DataTypes.LongType, true));
            _fields.add(1,DataTypes.createStructField("symbol", DataTypes.StringType, true));
            _fields.add(2,DataTypes.createStructField("price", DataTypes.DoubleType, true));
            _fields.add(3,DataTypes.createStructField("volume", DataTypes.LongType, true));
            StructType _schema = DataTypes.createStructType(_fields);

            if (rdd.count() > 0) {
                JavaRDD<String> _tick = rdd.map(x -> new String(x._2));
                JavaRDD<Row> _rowRDD = _tick.map(
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

                // Apply the schema to the RDD.
                DataFrame _tickDataFrame = sqlContext.createDataFrame(_rowRDD, _schema);

                // Register the DataFrame as a table.
                _tickDataFrame.registerTempTable("tick");

                _tickDataFrame.printSchema();

                try {
                    // SQL can be run over RDDs that have been registered as tables.
                    DataFrame _results = sqlContext.sql("SELECT symbol FROM tick");
                    JavaRDD javardd = _results.javaRDD();
                    List<String> _symbols = javardd.map((Function<Row, String>) row ->
                            row.getString(0)).collect();
                    _symbols.forEach(item -> System.out.println("Symbol : " + item));

                } catch (Exception e) {
                    System.out.println("ERROR: " + e.getMessage());
                }

            }

            rdd.foreach(record -> {
                byte[] data = record._2;
                Tick t = new Tick(data);
                datafile.printf("%s, %s, %.0f, %.2f\n", t.getDate(), t.getSymbolRoot(), t.getTradeVolume(), t.getTradePrice());

                String _data = new String(t.getData());
                System.out.println(_data);
            });

            System.out.println("------------");

            if (rdd.count() > 0) {
                //////////////////////////////////////////////////////
                // Test programmatically specifying a schema
                //////////////////////////////////////////////////////
                System.out.println("Parsing a json file. Schema will be auto detected.");
                DataFrame df = sqlContext.read().json("/user/mapr/tick.json");
                // Show the content of the DataFrame

                df.show(5);
                // Select only the "volume" column
                df.select("volume").show(5);
                // Select trades with volume larger than 100
                df.filter(df.col("volume").gt(100)).show(5);
                // Count trades by symbol
                df.groupBy("symbol").count().show(5);

                // Print the schema in a tree format
                df.printSchema();

                System.out.println("Parsing a txt file. Schema defined programmatically");
                // Load a text file and convert each line to a JavaBean.
                JavaRDD<String> tick = sc.textFile("/user/mapr/tick.txt");

                // The schema is encoded in a string
                String schemaString = "date symbol volume price";

                // Generate the schema based on the string of schema
                List<StructField> fields = new ArrayList<StructField>();
                for (String fieldName : schemaString.split(" ")) {
                    fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
                }
                StructType schema = DataTypes.createStructType(fields);

                // Convert records of the RDD (tick) to Rows.
                JavaRDD<Row> rowRDD = tick.map(
                        new Function<String, Row>() {
                            public Row call(String record) throws Exception {
                                if (record.trim().length() > 0) {
                                    String[] fields = record.split(" ");
                                    return RowFactory.create(fields[0], fields[1], fields[2], fields[3]);
                                } else {
                                    return RowFactory.create("", "", "", "");
                                }
                            }
                        });

                // Apply the schema to the RDD.
                DataFrame tickDataFrame = sqlContext.createDataFrame(rowRDD, schema);

                // Register the DataFrame as a table.
                tickDataFrame.registerTempTable("tick");

                // SQL can be run over RDDs that have been registered as tables.
                DataFrame results = sqlContext.sql("SELECT symbol FROM tick");

                // The results of SQL queries are DataFrames and support all the normal RDD operations.
                // The columns of a row in the result can be accessed by ordinal.
                List<String> symbols = results.javaRDD().map(new Function<Row, String>() {
                    public String call(Row row) {
                        return "Symbol: " + row.getString(0);
                    }
                }).take(100);
                symbols.forEach(item -> System.out.println(item));

                //////////////////////////////////////////////////////
                // Here's how to read in and query a json file
                //////////////////////////////////////////////////////
                // Create a DataFrame from a JSON source, specifying a schema.
                System.out.println("JSON: Schema programmatically specified.");
                DataFrame dataDF = sqlContext.read().schema(buildSchema()).json("/user/mapr/tick.json");
                //                sqlContext.jsonFile("/user/mapr/tick.json");

                dataDF.printSchema();
                // SQL can be run over RDDs that have been registered as tables.
                results = sqlContext.sql("SELECT price FROM tick");

                // The results of SQL queries are DataFrames and support all the normal RDD operations.
                // The columns of a row in the result can be accessed by ordinal.
                List<String> prices = results.javaRDD().map(new Function<Row, String>() {
                    public String call(Row row) {
                        return "Price: " + row.getString(0);
                    }
                }).take(100);
                prices.forEach(item -> System.out.println(item));


                //////////////////////////////////////////////////////
                // Here's another way to read in a file
                //////////////////////////////////////////////////////
                DataFrame df2 = sqlContext.jsonFile("/user/mapr/tick.json");
                System.out.println(df2.count());

                // reading a csv file only works with spark 2.0, but this is how you do it:
                //System.out.println(sqlContext.read().csv("/user/mapr/tick.csv").columns());
                //System.out.println(sqlContext.read().csv("/user/mapr/tick.csv").count());

                System.out.println("=============");
//            spark.read().csv("/home/mapr/tick.csv").count();


//            String l = [('Alice', 1)]
//            sqlContext.createDataFrame(l).collect()
//            sqlContext.createDataFrame(l, ['name', 'age']).collect()
//
//            sqlContext.createDataFrame(rdd).collect();
//            sqlContext.executeSql("SELECT COUNT(*)");
//
            }
        });

        // TODO: test getting offsets from another topic, to use for fetching a subset of a topic

        ssc.start();
        ssc.awaitTermination();
        closeDataFile();

    }
    /**
     * Build and return a schema to use for the sample data.
     */
    private static StructType buildSchema() {
        StructType schema = new StructType(
                new StructField[]{
                        DataTypes.createStructField("date", DataTypes.TimestampType, true),
                        DataTypes.createStructField("symbol", DataTypes.StringType, true),
                        DataTypes.createStructField("volume", DataTypes.IntegerType, true),
                        DataTypes.createStructField("price", DataTypes.DoubleType, true)
//                        DataTypes.createStructField("preferences",
//                                DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true), true),
//                        DataTypes.createStructField("registered_on", DataTypes.DateType, true),
//                        DataTypes.createStructField("visits",
//                                DataTypes.createArrayType(DataTypes.TimestampType, true), true)
                });
        return (schema);
    }
}