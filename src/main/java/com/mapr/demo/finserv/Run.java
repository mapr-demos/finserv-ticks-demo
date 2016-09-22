package com.mapr.demo.finserv;

import static com.mapr.demo.finserv.Producer.configureProducer;
import java.io.File;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us have a single executable as a build target.
 */
public class Run {

	public static void main(String[] args) throws IOException, Exception {
		Logger.getRootLogger().setLevel(Level.OFF);

		if (args.length < 1) {
			System.err.println("USAGE:\n"
				+ "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n"
				+ "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n");
		}
		switch (args[0]) {
			case "producer":
				Producer producer = getProducer(args);
				producer.produce();
				break;
			case "consumer":
				Consumer consumer = getConsumer(args);
				consumer.consume();
				break;
			default:
				throw new IllegalArgumentException("Don't know how to do " + args[0]);
		}
		System.exit(0);
	}

	private static Producer getProducer(final String args[]) {
		if (args.length < 3) {
			System.err.println("ERROR: You must specify the input data file and stream:topic.");
			System.err.println("USAGE:\n"
				+ "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source file | source directory] [stream:topic]\n"
				+ "Example:\n"
				+ "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer data/taqtrade20131218 /usr/mapr/taq:trades");
			System.exit(-1);
		}

		String topic = args[2];
		System.out.println("Publishing to topic: " + topic);

		configureProducer();
		File directory = new File(args[1]);
		return new Producer(topic, directory);
	}

	private static Consumer getConsumer(final String args[]) {
		if (args.length < 2) {
			System.err.println("ERROR: You must specify a stream:topic to consume data from.");
			System.err.println("USAGE:\n"
				+ "\tjava -cp `mapr classpath`:/mapr/ian.cluseter.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.demo.finserv.Run consumer [stream:topic] [NUM_THREADS] [verbose]\n"
				+ "Example:\n"
				+ "\tjava -cp `mapr classpath`:/mapr/ian.cluseter.com/user/mapr/nyse-taq-streaming-1.0-jar-with-dependencies.jar:/mapr/ian.cluster.com/user/mapr/resources/ com.mapr.demo.finserv.Run consumer /usr/mapr/taq:trades 2 verbose");
			System.exit(-1);
		}

		String topic = args[1];
		System.out.println("Subscribed to : " + topic);
		boolean verbose = false;
		int threadCount = 1;
		if ("verbose".equals(args[args.length - 1])) {
			verbose = true;
		}
		if (args.length == 4) {
			threadCount = Integer.valueOf(args[2]);
		}
		if (args.length == 3 && !"verbose".equals(args[2])) {
			threadCount = Integer.valueOf(args[2]);
		}

		return new Consumer(topic, verbose, threadCount);
	}
}
