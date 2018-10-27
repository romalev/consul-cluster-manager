package io.vertx.core;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.test.core.Repeat;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

// FIXME
public class ConsulClusteredComplexHATest extends ComplexHATest {


  @Test
  @Repeat(times = 10)
  public void testComplexFailover() {
    try {
      int numNodes = 8;
      createNodes(numNodes);
      deployRandomVerticles(() -> {
        killRandom();
      });
      await(10, TimeUnit.MINUTES);
    } catch (Throwable t) {
      // Need to explicitly catch throwables in repeats or they will be swallowed
      t.printStackTrace();
      // Don't forget to fail!
      fail(t.getMessage());
    }
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }
}
