package io.vertx.core.shareddata;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ConsulClusteredAsyncMapTest extends ClusteredAsyncMapTest {

  private ConsulClient consulClient;
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
    return new ConsulClusterManager(getConsulClientOptions());
  }

  @Override
  public void before() throws Exception {
    super.before();
    if (consulClient == null) {
      consulClient = ConsulClient.create(vertx, getConsulClientOptions());
    }
  }

  @Override
  public void after() throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    consulClient.deleteValues("foo", event -> latch.countDown());
    consulClient.deleteValues("bar", event -> latch.countDown());
    latch.await();
    super.after();
  }

  @Test
  public void testMapPutIfAbsentTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo", onSuccess(map -> {
      map.putIfAbsent("pipo", "molo", 10, onSuccess(vd -> {
        assertNull(vd);
        vertx.setTimer(150, l -> {
          System.out.println("Getting is executed");
          getVertx().sharedData().<String, String>getAsyncMap("foo", onSuccess(map2 -> {
            map2.get("pipo", onSuccess(res -> {
              assertNull(res);
              testComplete();
            }));
          }));
        });
      }));
    }));
    await();
  }

  @Test
  public void testMapPutTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo", onSuccess(map -> {
      map.put("pipo", "molo", 10, onSuccess(vd -> {
        vertx.setTimer(150, l -> {
          getVertx().sharedData().<String, String>getAsyncMap("foo", onSuccess(map2 -> {
            map2.get("pipo", onSuccess(res -> {
              assertNull(res);
              testComplete();
            }));
          }));
        });
      }));
    }));
    await();
  }

  private ConsulClientOptions getConsulClientOptions() {
    return new ConsulClientOptions()
      .setPort(port)
      .setHost("localhost");
  }
}
