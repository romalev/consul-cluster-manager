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
        registerSession()
                .compose(ConsulSessionTtlTesterA::registerKv)
                .compose(aVoid -> {
                    consulClient.getValue("Roman", resultHandler -> {
                        if (resultHandler.succeeded()) {
                            log.trace(resultHandler.result().getValue());
                        }
                    });
                    return Future.succeededFuture();
                });
    }

    private static Future<String> registerSession() {
        Future<String> future = Future.future();
        SessionOptions sessionOptions = new SessionOptions()
                .setTtl(30)
                .setName("testSession")

                .setBehavior(SessionBehavior.DELETE);

        consulClient.createSessionWithOptions(sessionOptions, idHandler -> {
            if (idHandler.succeeded()) {
                log.trace(idHandler.result());
                future.complete(idHandler.result());
            } else {
                future.fail(idHandler.cause());
            }

        });
        return future;
    }

    private static Future<Void> registerKv(String sessionId) {
        Future<Void> future = Future.future();
        consulClient.putValueWithOptions("Roman", "Lev", new KeyValueOptions()
                .setAcquireSession(sessionId), event -> {
            if (event.succeeded()) {
                log.trace("entry has been registered.");
                future.complete();
            } else {
                future.fail(event.cause());
            }
        });
        return future;
    }
}
