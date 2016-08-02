package com.mapr.demo.finserv;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;
import java.util.concurrent.*;

public class Consumer {

    // Declare a new consumer.
    public static KafkaConsumer consumer;

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("ERROR: You must specify a stream:topic to consume data from.");
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n" +
                    "Example:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer  /usr/mapr/taq:trades");

        }

        String topic =  args[1] ;
        System.out.println("Subscribed to : "+ topic);

        configureConsumer();

        List<String> topics = new ArrayList<String>();
        topics.add(topic);
        // Subscribe to the topic.
        consumer.subscribe(topics);

        int concurrencyFactor = 1;
        int poolSize = concurrencyFactor * Runtime.getRuntime().availableProcessors();

        ExecutorService es = Executors.newFixedThreadPool(poolSize);
        CompletionService<Boolean> parserService = new ExecutorCompletionService(es);

        List<ConsumerRecord> recordList = new ArrayList();
        long pollTimeOut = 1000;  // milliseconds
        //collect the processing result
        List<Boolean> resultList = new ArrayList();


        // https://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
        // This paradigm is an "at least once delivery" guarantee.
        // TODO: Is it okay for the listener #1 to potentially persist duplicate messages?

        long startTime = System.nanoTime();
        long last_update = 0;

        try {
            while (true) {
                // Request unread messages from the topic.
                ConsumerRecords<String, String> records = consumer.poll(pollTimeOut);
                if (records.count() == 0) {
                    System.out.println("No messages after " + pollTimeOut/1000 + " second wait. Total published = " + resultList.size());
                    Monitor.print_status(resultList.size(), poolSize, startTime);
                } else {

                    for (ConsumerRecord<String, String> record : records) {
                        recordList.add(record);
                        //collect records
                        if (recordList.size() == poolSize) {
                            int taskCount = poolSize;
                            //distribute these messages across the workers
                            recordList.forEach(recordToProcess -> parserService.submit(new ParsingWorker(recordToProcess)));
                            while (taskCount > 0) {
                                try {
                                    Future<Boolean> futureResult = parserService.take();
                                    if (futureResult != null) {
                                        boolean result = futureResult.get().booleanValue();
                                        resultList.add(result);
                                        taskCount = taskCount - 1;
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                            // TODO: uncomment this section if we want to avoid loosing messages
                            // verify all result are ok then commit it otherwise reprocess it.
//                            Map<TopicPartition, OffsetAndMetadata> commitOffset =
//                                    Collections.singletonMap(
//                                            new TopicPartition(record.topic(), record.partition()),
//                                            new OffsetAndMetadata(record.offset() + 1));
                            consumer.commitSync();
                            //clear all commit records from list
                            recordList.clear();
                        }

                    }

                    // Print performance stats once per second
                    if ((Math.floor(System.nanoTime() - startTime)/1e9) > last_update) {
                        last_update++;
                        Monitor.print_status(resultList.size(), poolSize, startTime);
                    }

                }

            }
        } catch (Throwable throwable) {
            System.err.printf("%s", throwable.getStackTrace());
        } finally {
            consumer.close();
            System.out.println("Consumed " + resultList.size() + " messages from stream.");
            System.out.println("Finished.");
        }

    }



    /* Set the value for configuration parameters.*/
    public static void configureConsumer() {
        Properties props = new Properties();
        props.put("enable.auto.commit","false");
        props.put("group.id", "mapr-workshop");
        props.put("client.id", UUID.randomUUID().toString());
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        //  which class to use to deserialize the value of each message
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
    }

}
