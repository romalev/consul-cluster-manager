package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.consul.ConsulAgent;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Test is written in fully async manner.
 *
 * @author Roman Levytskyi
 */
@RunWith(VertxUnitRunner.class)
public class ConsulSyncMapTest {

  private static final String MAP_NAME = "vertx-test.haInfo";
  private static final boolean isEmbeddedConsulAgentEnabled = false;
  @ClassRule
  public static RunTestOnContext rule = new RunTestOnContext();
  private static ConsulAgent consulAgent;
  private static ConsulClient consulClient;
  private static ConsulClientOptions cCOps;
  private static ConsulSyncMap<String, String> consulSyncMap;
  private static CacheManager cacheManager;

  static {
    System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
  }

  @BeforeClass
  public static void setUp(TestContext context) {
    Async async = context.async();
    rule.vertx().executeBlocking(event -> {
      consulAgent = new ConsulAgent();
      consulAgent.start();
      cCOps = new ConsulClientOptions().setPort(consulAgent.getPort());
      consulClient = ConsulClient.create(rule.vertx(), cCOps);
      cacheManager = new CacheManager(rule.vertx(), cCOps);
      event.complete();
    }, res ->
      createConsulSessionId()
        .compose(s -> {
          consulSyncMap = new ConsulSyncMap<>(MAP_NAME, "Roman", rule.vertx(), consulClient, cacheManager, s, new ConcurrentHashMap<>());
          return Future.succeededFuture();
        })
        .setHandler(event -> {
          if (event.succeeded()) {
            async.complete();
          } else {
            context.fail(event.cause());
          }
        }));
  }

  @AfterClass
  public static void tearDown(TestContext context) {
    cacheManager.close();
    rule.vertx().close(context.asyncAssertSuccess());
    consulAgent.stop();
  }

  private static Future<String> createConsulSessionId() {
    Future<String> future = Future.future();
    consulClient.createSession(future.completer());
    return future;
  }

  @Test
  public void verify_add(TestContext context) {
    Async async = context.async();
    // given
    String key = "keyA";
    String value = "localhost:keyA";
    // when
    rule.vertx().executeBlocking(event -> {
      consulSyncMap.put(key, value);
      sleep(1000L, context);
      event.complete();
    }, res ->
      consulClient.getValue(MAP_NAME + "/" + key, event -> {
        if (event.succeeded()) {
          try {
            ConsulEntry o = ConversationUtils.asConsulEntry(event.result().getValue());
            context.assertEquals(value, o.getValue());
            context.assertEquals(value, consulSyncMap.get(key));
          } catch (Exception e) {
            context.fail(e);
          }
          async.complete();
        } else {
          context.fail(event.cause());
        }
      }));
  }

  @Test
  public void verify_remove(TestContext context) {
    Async async = context.async();
    // given
    String key = "keyA";
    // when
    rule.vertx().executeBlocking(event -> {
      consulSyncMap.remove(key);
      sleep(2000L, context);
      event.complete();
    }, res ->
      consulClient.getValue(MAP_NAME + "/" + key, event -> {
        if (event.succeeded()) {
          context.assertNull(event.result().getValue());
          context.assertNull(consulSyncMap.get(key));
          async.complete();
        } else {
          context.fail(event.cause());
        }
      }));
  }

  private void sleep(Long sleepTime, TestContext context) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      context.fail(e);
    }
  }

}
