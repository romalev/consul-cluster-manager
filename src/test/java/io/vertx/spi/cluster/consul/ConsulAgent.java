package io.vertx.spi.cluster.consul;

import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Embedded consul agent.
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulAgent {
  private final Logger log = LoggerFactory.getLogger(ConsulAgent.class);

  private int port;
  private ConsulProcess consul;

  ConsulAgent() {
    this.port = Utils.getFreePort();
  }

  int start() {
    consul = ConsulStarterBuilder.consulStarter().withHttpPort(port).build().start();
    log.info("Consul test agent is up and running on port: " + port);
    return port;
  }

  void stop() {
    consul.close();
    log.info("Consul test agent has been stopped.");
  }

  public int getPort() {
    return port;
  }


}
