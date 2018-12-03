package io.vertx.core.shareddata;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class ConsulAsyncApBaseMultiMapTest extends ConsulAsyncBaseMultiMapTest {

  private static int port = 8500;

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
    JsonObject options = new JsonObject()
      .put("port", port)
      .put("host", "localhost")
      .put("preferConsistency", false);
    return new ConsulClusterManager(options);
  }
}
