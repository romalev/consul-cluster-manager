package io.vertx.core.shareddata;

import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link io.vertx.spi.cluster.consul.impl.ConsulLock}
 *
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public class ConsulClusteredAsynchronousLockTest extends ClusteredAsynchronousLockTest {

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
    return new ConsulClusterManager(getConsulClusterManagerOptions());
  }

  @Test
  public void testLockReleasedForClosedNode() throws Exception {
    super.testLockReleasedForClosedNode();
  }

  @Test
  public void testLockReleasedForKilledNode() throws Exception {
    super.testLockReleasedForKilledNode();
  }

  private JsonObject getConsulClusterManagerOptions() {
    return new JsonObject()
      .put("host", "localhost")
      .put("port", port);
  }
}
