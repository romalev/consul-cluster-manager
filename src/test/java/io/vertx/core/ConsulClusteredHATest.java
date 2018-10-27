package io.vertx.core;

import io.vertx.core.impl.VertxInternal;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.test.verticles.HAVerticle1;
import io.vertx.test.verticles.HAVerticle2;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ConsulClusteredHATest extends HATest {

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }

  @Test
  public void testHaGroups() throws Exception {
    vertx1 = startVertx("group1", 1);
    vertx2 = startVertx("group1", 1);
    vertx3 = startVertx("group2", 1);
    vertx4 = startVertx("group2", 1);
    CountDownLatch latch1 = new CountDownLatch(2);
    vertx1.deployVerticle("java:" + HAVerticle1.class.getName(), new DeploymentOptions().setHa(true), ar -> {
      assertTrue(ar.succeeded());
      assertTrue(vertx1.deploymentIDs().contains(ar.result()));
      latch1.countDown();
    });
    vertx3.deployVerticle("java:" + HAVerticle2.class.getName(), new DeploymentOptions().setHa(true), ar -> {
      assertTrue(ar.succeeded());
      assertTrue(vertx3.deploymentIDs().contains(ar.result()));
      latch1.countDown();
    });
    awaitLatch(latch1);
    CountDownLatch latch2 = new CountDownLatch(1);
    ((VertxInternal) vertx1).failoverCompleteHandler((nodeID, haInfo, succeeded) -> {
      fail("Should not failover here 1");
    });
    ((VertxInternal) vertx2).failoverCompleteHandler((nodeID, haInfo, succeeded) -> {
      fail("Should not failover here 2");
    });
    ((VertxInternal) vertx4).failoverCompleteHandler((nodeID, haInfo, succeeded) -> {
      assertTrue(succeeded);
      latch2.countDown();
    });
    ((VertxInternal) vertx3).simulateKill();
    awaitLatch(latch2);
    assertTrue(vertx4.deploymentIDs().size() == 1);
    CountDownLatch latch3 = new CountDownLatch(1);
    ((VertxInternal) vertx2).failoverCompleteHandler((nodeID, haInfo, succeeded) -> {
      assertTrue(succeeded);
      latch3.countDown();
    });
    ((VertxInternal) vertx4).failoverCompleteHandler((nodeID, haInfo, succeeded) -> {
      fail("Should not failover here 4");
    });
    ((VertxInternal) vertx1).simulateKill();
    awaitLatch(latch3);
    assertTrue(vertx2.deploymentIDs().size() == 1);
  }
}
