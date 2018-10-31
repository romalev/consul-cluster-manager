package io.vertx.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;

public class ConsulClusteredComplexHATest extends ComplexHATest {

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }
}
