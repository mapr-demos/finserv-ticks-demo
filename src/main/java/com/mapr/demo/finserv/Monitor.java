package com.mapr.demo.finserv;

import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by idownard on 7/27/16.
 */
public class Monitor {
    public static void print_status(long records_processed, int poolSize, long startTime) {
        long elapsedTime = System.nanoTime() - startTime;
//        Iterator<Future<RecordMetadata>> i = ParsingWorker.json_stream_futures.iterator();
//        long counter=0;
//        while (i.hasNext()) {
//            Future<RecordMetadata> f = i.next();
//            if (f != null && f.isDone())
//                counter++;
//        }
        System.out.printf("Thread: " + Thread.currentThread().getName() + ". Throughput = %.2f raw Kmsgs/sec consumed. Threads = %d. Total raw msgs consumed = %d. Total json msgs published: = %d\n",
                records_processed / ((double) elapsedTime / 1000000000.0) / 1000,
                poolSize, records_processed,
                Consumer.json_messages_published);
    }
}
