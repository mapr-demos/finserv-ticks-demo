package com.mapr.demo.finserv;

import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.ojai.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;

/**
 * Tests the effect of threading on message transmission to lots of topics
 */
@RunWith(Parameterized.class)
public class MaprDbSpeedTest {
    private static final double TIMEOUT = 30;  // seconds
    private static final int BATCH_SIZE = 100;  // The unit of measure for throughput is "batch size" per second
    // e.g. Throughput = X "hundreds of messages" per sec

    @BeforeClass
    public static void openDataFile() throws FileNotFoundException {
        data = new PrintWriter(new File("maprdb-count.csv"));
        data.printf("messageSize, threadCount, totalMsgsSent, elapasedSecs, aggregateTput, batchSecs, batchRate,bufferWrite\n");
    }

    @AfterClass
    public static void closeDataFile() {
        data.close();
    }

    private static PrintWriter data;

    @Parameterized.Parameters(name = "{index}: messageSize={0}, threads={1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {10, 1}, {100, 1}, {500, 1}, {1000, 1}, {2000, 1}, {5000, 1}, {10000, 1}
        });
    }

    private int threadCount = 1; // number of concurrent Kafka producers to run
    private int messageSize;  // size of each message sent into Kafka
    private static final Document end = MapRDB.newDocument(new HashMap<String, Object>());


//    private static final ProducerRecord<String, byte[]> end = new ProducerRecord<>("end", null);

    public MaprDbSpeedTest(int messageSize, int threadCount) {
        this.messageSize = messageSize;
        this.threadCount = threadCount;
    }

    @Test
    public void testThreads() throws Exception {
        System.out.printf("messageSize = %d, threadCount = %d\n", messageSize, threadCount);

        // Test with BUFFERWRITE disabled:
        Boolean bufferwrite = false;

        // Create new table names.
        String tableName = "speedTest";
        // delete the old table if it's there
        if (MapRDB.tableExists(tableName)) {
            System.out.println("deleting old table " + tableName);
            MapRDB.deleteTable(tableName);
        }
        // make a new table
        Table table = MapRDB.createTable(tableName);
        table.setOption(Table.TableOption.BUFFERWRITE, bufferwrite);

        Tick tick = new Tick("080845201DAA                T 00000000400000088400N0000000070800014CT1001100710051006");
        Document document = MapRDB.newDocument((Object) tick);

        double t0 = System.nanoTime() * 1e-9;
        double batchStart = 0;

        // We'll break out of this loop when that timeout occurs.
        long records_saved = 0;
        for (int i = 0; i >= 0 && i < Integer.MAX_VALUE; ) {
            for (int j = 0; j < BATCH_SIZE; j++) {
                table.insert(Long.toString(++records_saved), document);
            }
            i += BATCH_SIZE;
            double t = System.nanoTime() * 1e-9 - t0;
            double dt = t - batchStart;
            batchStart = t;
            // i = number of messages sent)
            // t = total elapsed time
            // i/t = overall throughput (number of messages sent overall per second)
            // dt = elapsed time for this batch
            // batch / dt = hundreds of messages sent per second for this batch
            data.printf("%d,%d,%d,%.3f,%.1f,%.3f,%.1f, %s\n", messageSize, threadCount, i, t, i / t, dt, BATCH_SIZE / dt, bufferwrite ? "true" : "false");
            if (t > TIMEOUT) {
                System.out.println("Average tput = " + i/t);
                break;
            }
        }



        // Now test with BUFFERWRITE enabled:
        bufferwrite = true;

        if (MapRDB.tableExists(tableName)) {
            System.out.println("deleting old table " + tableName);
            MapRDB.deleteTable(tableName);
        }
        // make a new table
        table = MapRDB.createTable(tableName);
        table.setOption(Table.TableOption.BUFFERWRITE, bufferwrite);

        t0 = System.nanoTime() * 1e-9;
        batchStart = 0;

        // We'll break out of this loop when that timeout occurs.
        records_saved = 0;
        for (int i = 0; i >= 0 && i < Integer.MAX_VALUE; ) {
            for (int j = 0; j < BATCH_SIZE; j++) {
                table.insert(Long.toString(++records_saved), document);
            }
            i += BATCH_SIZE;
            double t = System.nanoTime() * 1e-9 - t0;
            double dt = t - batchStart;
            batchStart = t;
            // i = number of messages sent)
            // t = total elapsed time
            // i/t = overall throughput (number of messages sent overall per second)
            // dt = elapsed time for this batch
            // batch / dt = hundreds of messages sent per second for this batch
            data.printf("%d,%d,%d,%.3f,%.1f,%.3f,%.1f, %s\n", messageSize, threadCount, i, t, i / t, dt, BATCH_SIZE / dt, bufferwrite ? "true" : "false");
            if (t > TIMEOUT) {
                System.out.println("Average tput = " + i/t);
                break;
            }
        }
    }
}
