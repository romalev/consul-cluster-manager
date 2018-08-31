package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.AsyncMap;
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

/**
 * Testing {@link ConsulAsyncMap}.
 *
 * @author Roman Levytskyi
 */
@RunWith(VertxUnitRunner.class)
public class ConsulAsyncMapTest {

    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final String MAP_NAME = "vertx-test.async";

    private static ConsulAgent consulAgent;
    private static ConsulClient consulClient;
    private static ConsulClientOptions cCOps;
    private static AsyncMap<String, String> consulAsyncMap;

    @ClassRule
    public static RunTestOnContext rule = new RunTestOnContext();

    @BeforeClass
    public static void setUp(TestContext context) {
        Async async = context.async();
        rule.vertx().executeBlocking(event -> {
            consulAgent = new ConsulAgent();
            cCOps = new ConsulClientOptions().setPort(consulAgent.getPort());
            consulClient = ConsulClient.create(rule.vertx(), cCOps);
            CacheManager.init(rule.vertx(), cCOps);
            consulAsyncMap = new ConsulAsyncMap<>(MAP_NAME, Vertx.vertx(), consulClient);
            event.complete();
        }, res -> {
            if (res.succeeded()) {
                async.complete();
            } else {
                context.fail(res.cause());
            }
        });
    }

    @Test
    public void verify_put(TestContext context) {

    }

    public void verify_putWithTtl(TestContext context) {

    }

    public void verify_putIfAbsent(TestContext context) {

    }

    public void verify_putIfAbsentWithTtl(TestContext context) {

    }

    public void verify_remove(TestContext context) {

    }

    public void verify_removeIfPresent(TestContext context) {

    }

    public void verify_replace(TestContext context) {

    }

    public void verifu_replaceIfPresent(TestContext context) {

    }


    @AfterClass
    public static void tearDown(TestContext context) {
        CacheManager.close();
        consulAgent.stop();
        rule.vertx().close(context.asyncAssertSuccess());
    }
}
