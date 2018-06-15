package io.vertx.spi.cluster.consul.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

public class AvailablePortFinder {

    private final static Logger log = LoggerFactory.getLogger(AvailablePortFinder.class);

    public static int find(int from, final int to) {
        // TODO : port validation
        log.debug("Trying to find available port from: '{}' to: '{}'", from, to);

        int availablePort = -1;

        while (from < to) {
            if (available(from)) {
                availablePort = from;
                break;
            }
            from++;
        }

        if (availablePort == -1) {
            log.warn("Couldn't find available port in range from: '{}', to '{}'", from, to);
        } else {
            log.info("Available port is: '{}'", availablePort);
        }
        return availablePort;
    }

    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port number to check for availability
     * @return <tt>true</tt> if the port is available, or <tt>false</tt> if not
     * @throws IllegalArgumentException is thrown if the port number is out of range
     */
    public static boolean available(int port) throws IllegalArgumentException {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            ss = new ServerSocket(port);
            ss.setReuseAddress(true);
            ds = new DatagramSocket(port);
            ds.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            // Do nothing
        } finally {
            if (ds != null) {
                ds.close();
            }
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }
        return false;
    }
}
