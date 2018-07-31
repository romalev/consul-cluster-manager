package io.vertx.spi.cluster.consul.examples.testing;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;

public class ConsulSessionTtlTesterA {
    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final Logger log = LoggerFactory.getLogger(ConsulSessionTtlTesterA.class);

    private static Vertx vertx = Vertx.vertx();
    private static ConsulClient consulClient = ConsulClient.create(vertx);

    public static void main(String[] args) {

        registerSession(10)
                .compose(s -> registerKv(s, "Roman", "Lev"))
                .compose(s -> registerKv(s, "Roman", "Updated"))
                .compose(aVoid -> registerSession(30))
                .compose(s -> registerKv(s, "Roman", "Lev1"))
                .compose(aVoid -> {
                    return Future.succeededFuture();
                });
    }

    private static Future<String> registerSession(int ttl) {
        Future<String> future = Future.future();
        SessionOptions sessionOptions = new SessionOptions()
                .setTtl(ttl)
                .setName("testSession")
                .setBehavior(SessionBehavior.DELETE);

        consulClient.createSessionWithOptions(sessionOptions, idHandler -> {
            if (idHandler.succeeded()) {
                log.trace(idHandler.result());
                future.complete(idHandler.result());
            } else {
                log.trace(idHandler.cause().toString());
                future.fail(idHandler.cause());
            }

        });
        return future;
    }

    private static Future<String> registerKv(String sessionId, String key, String value) {
        Future<String> future = Future.future();
        consulClient.putValueWithOptions(key, value, new KeyValueOptions()
                .setAcquireSession(sessionId), event -> {
            if (event.succeeded()) {
                log.trace("entry has been registered.");
                future.complete(sessionId);
            } else {
                future.fail(event.cause());
            }
        });
        return future;
    }
}
