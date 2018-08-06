package io.vertx.spi.cluster.consul.examples.testing;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;
import io.vertx.spi.cluster.consul.impl.ConsulLock;

import java.util.concurrent.CountDownLatch;

public class ConsulLockTester {

    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    static Vertx vertx = Vertx.vertx();
    static ConsulClient consulClient = ConsulClient.create(vertx);

    public static void main(String[] args) {

        ConsulLock consulLock = new ConsulLock("lock", 10000, consulClient);
        ConsulLock consulLock1 = new ConsulLock("lock", 10000, consulClient);

        consulLock.release();

    }

    static String sessionId1;
    static String sessionId2;

    private static void case2() {
        CountDownLatch c0 = new CountDownLatch(1);

        getTtlSessionId(0, "testSession").setHandler(event -> {
            sessionId2 = event.result();
            c0.countDown();
        });

        try {
            c0.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        CountDownLatch c1 = new CountDownLatch(1);

        getTtlSessionId(0, "testSession").setHandler(event -> {
            sessionId1 = event.result();
            c1.countDown();
        });

        try {
            c1.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        CountDownLatch c2 = new CountDownLatch(1);

        System.out.println("session is" + sessionId1);

        consulClient.putValueWithOptions("key", "a", new KeyValueOptions().setAcquireSession(sessionId1), event -> {
            System.out.println(event);
            c2.countDown();
        });

        try {
            c2.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        consulClient.putValueWithOptions("key", "b", new KeyValueOptions().setAcquireSession(sessionId2), event -> {
            System.out.println(event);
        });
    }

    static Future<String> getTtlSessionId(long ttl, String name) {
        String sessionName = "ttlSession_" + name;
        Future<String> future = Future.future();
        SessionOptions sessionOpts = new SessionOptions()
                // .setTtl(TimeUnit.MILLISECONDS.toSeconds(ttl))
                .setBehavior(SessionBehavior.DELETE)
                .setName(sessionName);
        consulClient.createSessionWithOptions(sessionOpts, idHandler -> {
            if (idHandler.succeeded()) {

                future.complete(idHandler.result());
            } else {
                future.fail(idHandler.cause());
            }
        });
        return future;
    }

    private static void case1() {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        consulClient.putValueWithOptions("key", "a", new KeyValueOptions(), event -> {
            System.out.println(event);
            countDownLatch.countDown();
        });

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        consulClient.putValueWithOptions("key", "b", new KeyValueOptions().setCasIndex(0), event -> {
            System.out.println(event);
        });
    }
}
