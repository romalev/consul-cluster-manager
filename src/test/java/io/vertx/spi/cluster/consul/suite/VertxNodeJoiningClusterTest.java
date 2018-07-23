package io.vertx.spi.cluster.consul.suite;

import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.spi.cluster.consul.ConsulAgent;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.spi.cluster.consul.verticles.AliceVerticle;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@RunWith(VertxUnitRunner.class)
public class VertxNodeJoiningClusterTest {

    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private final Logger log = LoggerFactory.getLogger(VertxNodeJoiningClusterTest.class);

    private ConsulAgent consulAgent = new ConsulAgent();
    private ConsulClientOptions consulClientOptions = new ConsulClientOptions()
            .setPort(consulAgent.getPort());
    private ConsulClusterManager consulClusterManager = new ConsulClusterManager(consulClientOptions);

    // custom supplied to create clustered
    private Supplier<Vertx> clusteredVertxSupplier = () -> {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Vertx> vertxAtomicReference = new AtomicReference<>();
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setClusterManager(consulClusterManager);
        Vertx.clusteredVertx(vertxOptions, handler -> {
            if (handler.succeeded()) {
                vertxAtomicReference.set(handler.result());
                log.trace("Clustered vertx instance has been created.");
                latch.countDown();
            } else {
                log.error("Unable to create vertx instance due to: " + handler.cause().toString());
                throw new VertxException("Unable to create clustered Vertx");
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new VertxException(e);
        }
        return vertxAtomicReference.get();
    };

    @Rule
    public RunTestOnContext rule = new RunTestOnContext(clusteredVertxSupplier);
    private Vertx vertx;


    @Before
    public void setUp(TestContext context) {
        vertx = rule.vertx();
        vertx.deployVerticle(new AliceVerticle(), event -> {
            if (event.succeeded()) context.async().complete();
            else context.fail(event.cause());
        });
    }


    @Test
    public void verifyJoinOperation(TestContext testContext) {
        testContext.async().complete();
    }


    @After
    public void tearDown(TestContext testContext) {
        log.trace("Tearing down...");
        CompletableFuture completableFuture = new CompletableFuture();
        vertx.close(event -> {
            if (event.succeeded()) completableFuture.complete(true);
            else completableFuture.completeExceptionally(event.cause());
        });

        try {
            completableFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            log.error(e);
            throw new VertxException(e);
        } finally {
            consulAgent.stop();
            testContext.async().complete();
        }
    }


}
