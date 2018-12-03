package io.vertx.core.shareddata;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.CountDownLatch;

public class ConsulClusteredSharedCounterTest extends ClusteredSharedCounterTest {

  private ConsulClient consulClient;
  private static int port;

  @BeforeClass
  public static void startConsulCluster() {
    port = ConsulCluster.init();
  }

  @AfterClass
  public static void shutDownConsulCluster() {
    ConsulCluster.shutDown();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(getClusterManagerOptions());
  }

  @Override
  public void before() throws Exception {
    super.before();
    if (consulClient == null) {
      consulClient = ConsulClient.create(vertx, new ConsulClientOptions(getClusterManagerOptions()));
    }
  }

  @Override
  public void after() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    consulClient.deleteValues("__vertx.counters", event -> latch.countDown());
    latch.await();
    super.after();
  }

  private JsonObject getClusterManagerOptions() {
    return new JsonObject()
      .put("host", "localhost")
      .put("port", port);
  }

}
