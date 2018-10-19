package io.vertx.core.shareddata;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;

public class ConsulClusteredAsynchronousLockTest extends ClusteredAsynchronousLockTest {

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }
}
