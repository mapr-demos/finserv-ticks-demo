package com.mapr.demo.finserv;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * Pick whether we want to run as producer or consumer. This lets us
 * have a single executable as a build target.
 */
public class Run {
    public static void main(String[] args) throws IOException {
        Logger.getRootLogger().setLevel(Level.OFF);

        if (args.length < 1) {
            System.err.println("USAGE:\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run producer [source data file] [stream:topic]\n" +
                    "\tjava -cp `mapr classpath`:./nyse-taq-streaming-1.0-jar-with-dependencies.jar com.mapr.examples.Run consumer [stream:topic]\n");
        }
        switch (args[0]) {
            case "producer":
                Producer.main(args);
                break;
            case "consumer":
                Consumer.main(args);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
}