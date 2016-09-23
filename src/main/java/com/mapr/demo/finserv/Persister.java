/*
 * Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved
 */
package com.mapr.demo.finserv;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.mapr.db.MapRDB;
import com.mapr.db.Table;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Persister {

	private static final Logger LOG = LoggerFactory.getLogger(Persister.class);

	// Declare a new consumer.
	private static KafkaConsumer consumer;

	// every N rows print a MapR-DB update
	private static final int U_INTERVAL = 100;

	// polling
	private static final int TIMEOUT = 1000;

	public static void main(String[] args) throws IOException {
		configureConsumer(args);

		// we will listen to everything in JSON format after it's
		// been processed
		String topic = "/user/mapr/taq:processed";
		String tableName = "/user/mapr/ticktable";
		Set<String> syms = new HashSet<>();
		long nmsgs = 0;
		Table table;

		if (args.length == 1) {
			topic = args[0];
		}

		List<String> topics = new ArrayList<>();
		topics.add(topic);

		// subscribe to the raw data
		System.out.println("subscribing to " + topic);
		consumer.subscribe(topics);

		// delete the old table if it's there
		if (MapRDB.tableExists(tableName)) {
			System.out.println("deleting old table " + tableName);
			MapRDB.deleteTable(tableName);
		}

		// make a new table
		table = MapRDB.createTable(tableName);

		// probably want this
		table.setOption(Table.TableOption.BUFFERWRITE, true);

		// request everything
		for (;;) {
			ConsumerRecords<String, String> msg = consumer.poll(TIMEOUT);
			if (msg.count() == 0) {
				System.out.println("No messages after 1 second wait.");
			}
			else {
				System.out.println("Read " + msg.count() + " messages");
				nmsgs += msg.count();

				// Iterate through returned records, extract the value
				// of each message, and print the value to standard output.
				Iterator<ConsumerRecord<String, String>> iter = msg.iterator();
				while (iter.hasNext()) {
					ConsumerRecord<String, String> record = iter.next();

					// XXX won't have this problem when string is encoded correctly - just
					// XXX for testing
					String cleaned = record.value().replaceAll("\\p{Cntrl}", "");

					// use the _id field in the msg
					Document document = MapRDB.newDocument(cleaned);

					String this_sym = document.getString("symbol");
					syms.add(this_sym);

					// save document into the table
					table.insertOrReplace(document);
				}
			}

			if ((msg.count() != 0) && (nmsgs % U_INTERVAL) == 0) {
				System.out.println("Write update per-symbol:");
				System.out.println("------------------------");

				for (String s : syms) {
					QueryCondition cond = MapRDB.newCondition()
						.is("symbol", QueryCondition.Op.EQUAL, s).build();
					DocumentStream results = table.find(cond);
					int c = 0;
					for (Document d : results) {
						c++;
					}
					System.out.println("\t" + s + ": " + c);
				}
			}
		}
	}

	/*
	 * cause consumers to start at beginning of topic on first read
	 */
	private static void configureConsumer(String[] args) {
		Properties props = new Properties();
		props.put("auto.offset.reset", "earliest");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<>(props);
	}
}
