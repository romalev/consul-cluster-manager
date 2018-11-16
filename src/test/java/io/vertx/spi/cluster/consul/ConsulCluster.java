package io.vertx.spi.cluster.consul;

/**
 * Mock of consul cluster consisting of only one consul agent.
 */
public class ConsulCluster {

  private static ConsulAgent consulAgent;

  public static int init() {
    consulAgent = new ConsulAgent();
    int port = consulAgent.start();
    Runtime.getRuntime().addShutdownHook(new Thread(consulAgent::stop));
    return port;
  }

  public static void shutDown() {
    consulAgent.stop();
  }
}
