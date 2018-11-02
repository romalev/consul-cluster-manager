package io.vertx.core.shareddata;

import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

public class ConsulClusteredAsyncMapTest extends ClusteredAsyncMapTest {

  private ConsulClient consulClient;

  @Override
  public void before() throws Exception {
    super.before();
    if (consulClient == null) {
      consulClient = ConsulClient.create(vertx);
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

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(new ConsulClientOptions());
  }

  /**
   * Consul restriction: TTL value (on entries) must be between 10s and 86400s currently. [Invalidation-time is twice the TTL time](https://github.com/hashicorp/consul/issues/1172)
   * this means actual time when ttl entry gets removed (expired) is doubled to what you will specify as a ttl.
   */
  @Test
  public void testMapPutTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo", onSuccess(map -> {
      map.put("pipo", "molo", 10, onSuccess(vd -> {
        vertx.setTimer(20000, l -> {
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
  public void testMapPutIfAbsentTtl() {
    getVertx().sharedData().<String, String>getAsyncMap("foo", onSuccess(map -> {
      map.putIfAbsent("pipo", "molo", 10, onSuccess(vd -> {
        assertNull(vd);
        vertx.setTimer(20000, l -> {
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

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      fail(e);
    }
  }

}
