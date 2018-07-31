package io.vertx.spi.cluster.consul.examples.testing;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ConsulClient;

public class ConsulSessionTtlTesterB {
    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final Logger log = LoggerFactory.getLogger(ConsulSessionTtlTesterB.class);

    private static Vertx vertx = Vertx.vertx();
    private static ConsulClient consulClient = ConsulClient.create(vertx);

    public static void main(String[] args) {
        consulClient.getValue("Roman", event -> {
            if (event.succeeded()) {
                log.trace("got by Roman :'{}'", event.result().getValue());
                log.trace("session id Roman :'{}'", event.result().getSession());

                // KeyValueOptions keyValueOptions = new KeyValueOptions().setAcquireSession(event.result().getSession().set);

                consulClient.putValue("Roman", "Updated", resultHandler -> {
                    if (resultHandler.succeeded()) {
                        log.trace("updated");
                    }
                });

            }
        });
    }


}
