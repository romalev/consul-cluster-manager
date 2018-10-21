package io.vertx.core.shareddata;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;

import java.util.concurrent.CountDownLatch;

public class ConsulClusteredSharedCounterTest extends ClusteredSharedCounterTest {

  private ConsulClient consulClient;

  public void before() throws Exception {
    super.before();
    if (consulClient == null) {
      consulClient = ConsulClient.create(vertx);
    }
  }

  @Override
  public void after() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    consulClient.deleteValues("__vertx.counters", event -> latch.countDown());
    latch.await();
    super.after();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }
}
