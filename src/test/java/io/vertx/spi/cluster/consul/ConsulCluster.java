package io.vertx.spi.cluster.consul;

import io.vertx.core.VertxException;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Mock of consul cluster consisting of only one consul agent.
 */
public class ConsulCluster {

  private static ConsulAgent consulAgent;
  private static final AtomicBoolean started = new AtomicBoolean();

  public static int init() {
    if (started.compareAndSet(false, true)) {
      consulAgent = new ConsulAgent();
      return consulAgent.start();
    } else {
      throw new VertxException("Cluster has been already started!");
    }
  }

  public static void shutDown() {
    if (started.compareAndSet(true, false)) {
      consulAgent.stop();
    } else {
      throw new VertxException("Cluster has been already stopped!");
    }
  }
}
