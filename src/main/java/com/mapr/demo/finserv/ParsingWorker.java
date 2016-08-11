package com.mapr.demo.finserv;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.text.ParseException;
import java.util.LinkedList;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class ParsingWorker implements Callable<Boolean> {

    ConsumerRecord record;
    static long records_processed = 0L;
    public static LinkedList<Future<RecordMetadata>> json_stream_futures = new LinkedList<>();

    public ParsingWorker(ConsumerRecord record) {
        this.record = record;
    }

    public Boolean call() {

        try {
            // processing steps go here
            JSONObject json = parse((String)record.value());
            Future<RecordMetadata> write_status = Publish((String)json.get("sender"), "/user/mapr/taq:"+json.get("sender"), json);
            json_stream_futures.add(write_status);
            //RecordMetadata result = write_status.get();
//            System.out.println("publish returned: " + result.topic() + " partition " + result.partition() + " offset " + result.offset());
            System.out.println("json_stream_futures size = " + json_stream_futures.size());
            records_processed ++;  //TODO: I don't think this is thread safe.
            return Boolean.TRUE;
        } catch (Exception e) {
            e.printStackTrace();
            return Boolean.FALSE;
        }
    }

    private static Future<RecordMetadata> Publish(String key, String topic, JSONObject json) {

        Properties props = new Properties();
        // TODO: use a JSON serializer
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(props);
//        System.out.println("Writing to topic " + topic + " from thread " + Thread.currentThread().getName() + ".");

        ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic, json.toString());

        // Send the record to the producer client library.
        return(producer.send(rec));

    }

    private static JSONObject parse(String record) throws ParseException {
        if (record.length() < 71) {
            throw new ParseException("Expected line to be at least 71 characters, but got " + record.length(), record.length());
        }

        JSONObject trade_info = new JSONObject();
        trade_info.put("date", record.substring(0, 9));
        trade_info.put("exchange", record.substring(9, 10));
        trade_info.put("symbol root", record.substring(10, 16).trim());
        trade_info.put("symbol suffix", record.substring(16, 26).trim());
        trade_info.put("saleCondition", record.substring(26, 30).trim());
        trade_info.put("tradeVolume", record.substring(30, 39));
        trade_info.put("tradePrice", record.substring(39, 46) + "." + record.substring(46, 50));
        trade_info.put("tradeStopStockIndicator", record.substring(50, 51));
        trade_info.put("tradeCorrectionIndicator", record.substring(51, 53));
        trade_info.put("tradeSequenceNumber", record.substring(53, 69));
        trade_info.put("tradeSource", record.substring(69, 70));
        trade_info.put("tradeReportingFacility", record.substring(70, 71));
        if (record.length() >= 74) {
            trade_info.put("sender", record.substring(71, 75));

            JSONArray receiver_list = new JSONArray();
            int i = 0;
            while (record.length() >= 78 + i) {
                receiver_list.add(record.substring(75 + i, 79 + i));
                i += 4;
            }
            trade_info.put("receivers", receiver_list);
        }
        return trade_info;

    }

}