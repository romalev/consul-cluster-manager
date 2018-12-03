package io.vertx.spi.cluster.consul;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Test utils.
 */
public class Utils {

  public static int getFreePort() {
    int port = -1;
    try {
      ServerSocket socket = new ServerSocket(0);
      port = socket.getLocalPort();
      socket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return port;
  }

}
