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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(MaprDbSpeedTest.class);
    private static final double TIMEOUT = 30;  // seconds
    private static final int BATCH_SIZE = 1000;  // The unit of measure for throughput is "batch size" per second
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
                {100, 1}, {1000, 1}, {10000, 1}, {100000, 1},
                {100, 2}, {1000, 2}, {10000, 2}, {100000, 2},
                {100, 4}, {1000, 4}, {10000, 4}, {100000, 4},
                {100, 8}, {1000, 8}, {10000, 8}, {100000, 8},
                {100, 16}, {1000, 16}, {10000, 16}, {100000, 16},
                {100, 32}, {1000, 32}, {10000, 32}, {100000, 32}
        });
    }

    private int threadCount = 1; // number of concurrent Kafka producers to run
    private int messageSize;  // size of each message sent into Kafka
    private static final Document end = MapRDB.newDocument(new HashMap<String, Object>());

    long records_saved = 0;
//    private static final ProducerRecord<String, byte[]> end = new ProducerRecord<>("end", null);

    public MaprDbSpeedTest(int messageSize, int threadCount) {
        this.messageSize = messageSize;
        this.threadCount = threadCount;
    }

    private static class Sender extends Thread {
        private final BlockingQueue<Document> queue;
        private final Table table;

        private Sender(Table table, BlockingQueue<Document> queue) {
            this.queue = queue;
            this.table = table;
        }

        @Override
        public void run() {
            long count = 0;
            try {
                Document doc = queue.take();
                while (doc != end) {
                    table.insertOrReplace(this.getName()+"-"+Long.toString(++count), doc);
                    doc = queue.take();
                }
            } catch (InterruptedException e) {
                LOG.error(this.getName() + ": Interrupted\n");
            }
        }
    }

    @Test
    public void testThreads() throws Exception {
        LOG.info("messageSize = " + messageSize + ", threadCount = " + threadCount + "\n");

        // Test with BUFFERWRITE enabled:
        Boolean bufferwrite = true;

        // Create new table names.
        String tableName = "speedTest";
        // delete the old table if it's there
        if (MapRDB.tableExists(tableName)) {
            MapRDB.deleteTable(tableName);
        }
        // make a new table
        MapRDB.createTable(tableName);

        String tick_data = "080845201DAA                T 00000000400000088400N0000000070800014CT10011007100510061007";
        char[] space = new char[messageSize - tick_data.length()];
        Arrays.fill(space, '*');
        Tick tick = new Tick(tick_data + new String(space));
        Document document = MapRDB.newDocument((Object) tick);

        // Create a pool of sender threads.
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        // We need some way to give each sender messages to publish.
        // We'll do that via this list of queues.
        List<BlockingQueue<Document>> queues = Lists.newArrayList();
        for (int i = 0; i < threadCount; i++) {
            // We use BlockingQueue to buffer messages for each sender.
            // We use this type not for concurrency reasons (although it is thread safe) but
            // rather because it provides an efficient way for senders to take messages if
            // they're available and for us to generate those messages (see below).
            BlockingQueue<Document> q = new ArrayBlockingQueue<>(BATCH_SIZE*2);
            queues.add(q);
            Table table = MapRDB.getTable(tableName);
            table.setOption(Table.TableOption.BUFFERWRITE, bufferwrite);
            // spawn each thread with a reference to "q", which we'll add messages to later.
            pool.submit(new Sender(table, q));
        }

        double t0 = System.nanoTime() * 1e-9;
        double batchStart = 0;

        // We'll break out of this loop when that timeout occurs.
        for (int i = 0; i >= 0 && i < Integer.MAX_VALUE; ) {
            for (int j = 0; j < BATCH_SIZE; j++) {
                int qid = j % threadCount;
                try {
                    // Put a message to be published in the queue belonging to the sender we just selected.
                    // That sender will automatically send this message as soon as possible.
                    queues.get(qid).put(document);
                } catch (Exception e) {
                    // BlockingQueue might throw an IllegalStateException if the queue fills up.
                    e.printStackTrace();
                }
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
                LOG.info("buffer = "+ bufferwrite +", messageSize = " + messageSize + ", Average tput = " + i/t);
                break;
            }
        }
        for (int i = 0; i < threadCount; i++) {
            queues.get(i).add(end);
        }
        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        if (MapRDB.tableExists(tableName)) {
            MapRDB.deleteTable(tableName);
        }


        // TODO test with BUFFERWRITE disabled:

    }
}
