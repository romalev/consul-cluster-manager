package io.vertx.spi.cluster.consul.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class AliceVerticle extends AbstractVerticle {

    private static final Logger log = LoggerFactory.getLogger(AliceVerticle.class);

    @Override
    public void start(Future<Void> startFuture) {
        log.trace("Starting: " + AliceVerticle.class.getSimpleName());
        startFuture.complete();
    }

    @Override
    public void stop(Future<Void> stopFuture) {
        log.trace("Stopping : " + AliceVerticle.class.getSimpleName());
        stopFuture.complete();
    }
}
