package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Test for {@link ConsulSyncMap}
 *
 * @author Roman Levytskyi
 */
public class ConsulSyncMapTest {

  private Vertx vertx;
  private ConsulClient consulClient;
  private String sessionId;

  @Before
  public void setUp() {
    vertx = Vertx.vertx();
    consulClient = ConsulClient.create(vertx);
    sessionId = getSessionId();
  }

  @Test
  public void syncMapOperation() {
    String k = "myKey";
    String v = "myValue";

    ConsulSyncMap<String, String> syncMap = new ConsulSyncMap<>("syncMapTest", "testSyncMapNodeId", vertx, consulClient);

    syncMap.put(k, v);
    assertFalse(syncMap.isEmpty());

    assertEquals(syncMap.get(k), v);

    assertTrue(syncMap.size() > 0);
    assertTrue(syncMap.containsKey(k));
    assertTrue(syncMap.containsValue(v));

    assertTrue(syncMap.keySet().contains(k));
    assertTrue(syncMap.values().contains(v));

    syncMap.entrySet().forEach(entry -> {
      assertEquals(k, entry.getKey());
      assertEquals(v, entry.getValue());
    });

    String value = syncMap.remove(k);
    assertEquals(value, v);
    assertNull(syncMap.get(k));

    syncMap.clear();
    assertTrue(syncMap.isEmpty());

  }

  @After
  public void tearDown() {
    destroySessionId();
    vertx.close();
    consulClient.close();
  }

  private String getSessionId() {
    CompletableFuture<String> future = new CompletableFuture<>();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(SessionBehavior.DELETE)
      .setLockDelay(0) // can't specify 0 - perhaps bug in consul client implementation.
      .setName("test");
    consulClient.createSessionWithOptions(sessionOptions, resultHandler -> {
      if (resultHandler.succeeded()) future.complete(resultHandler.result());
      else future.completeExceptionally(resultHandler.cause());
    });

    String sId = null;
    try {
      sId = future.get(2000, TimeUnit.MILLISECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException e) {
      fail(e.getMessage());
    }
    return sId;
  }

  private void destroySessionId() {
    CountDownLatch latch = new CountDownLatch(1);
    consulClient.destroySession(sessionId, event -> latch.countDown());
    try {
      latch.await(2000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      fail(e.getMessage());
    }
  }
}
