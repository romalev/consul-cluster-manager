package io.vertx.spi.cluster.consul;

import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;

public class ConsulAgent {

  // slf4j
  static {
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
  }

  private final Logger log = LoggerFactory.getLogger(ConsulAgent.class);

  private int port;
  private ConsulProcess consul;

  public ConsulAgent() {
    this.port = getFreePort();
  }

  public void start() {
    consul = ConsulStarterBuilder.consulStarter().withHttpPort(port).build().start();
    log.info("Consul test agent is up and running on port: " + port);
  }

  public void stop() {
    consul.close();
    log.info("Consul test agent has been stopped.");
  }

  public int getPort() {
    return port;
  }

  private int getFreePort() {
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
