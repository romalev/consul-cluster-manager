package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.consul.ConsulAgent;
import org.junit.*;
import org.junit.runner.RunWith;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Test is written in fully async manner.
 *
 * @author Roman Levytskyi
 */
@RunWith(VertxUnitRunner.class)
public class ConsulSyncMapTest {

    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final String MAP_NAME = "vertx-test.haInfo";

    private static ConsulAgent consulAgent;
    private static ConsulClient consulClient;
    private static ConsulClientOptions cCOps;
    private static Watch<KeyValueList> watch;
    private static ConsulSyncMap<String, String> consulSyncMap;

    @ClassRule
    public static RunTestOnContext rule = new RunTestOnContext();

    @BeforeClass
    public static void setUp(TestContext context) {
        Async async = context.async();
        rule.vertx().executeBlocking(event -> {
            if (consulAgent == null) {
                consulAgent = new ConsulAgent();
            }
            cCOps = new ConsulClientOptions().setPort(consulAgent.getPort());
            consulClient = ConsulClient.create(rule.vertx(), cCOps);
            watch = Watch.keyPrefix(MAP_NAME, rule.vertx(), cCOps);
            event.complete();
        }, res ->
                createConsulSessionId()
                        .compose(s -> {
                            consulSyncMap = new ConsulSyncMap<>(MAP_NAME, rule.vertx(), consulClient, watch, s, new ConcurrentHashMap<>());
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


    @Test
    public void verifyAdd(TestContext context) {
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
                            context.assertEquals(value, ClusterSerializationUtils.decode(event.result().getValue()));
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
    public void verifyRemove(TestContext context) {
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

    @AfterClass
    public static void tearDown(TestContext context) {
        watch.stop();
        consulAgent.stop();
        rule.vertx().close(context.asyncAssertSuccess());
    }

    private static Future<String> createConsulSessionId() {
        Future<String> future = Future.future();
        consulClient.createSession(future.completer());
        return future;
    }

    private static void sleep(Long sleepTime, TestContext context) {
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            context.fail(e);
        }
    }

}
