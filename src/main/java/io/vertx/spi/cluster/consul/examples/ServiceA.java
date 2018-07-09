package io.vertx.spi.cluster.consul.examples;


import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.spi.cluster.consul.impl.AvailablePortFinder;
import io.vertx.spi.cluster.consul.impl.ConsulClusterManagerOptions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class ServiceA {

    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final Logger log = LoggerFactory.getLogger(ServiceA.class);
    private static Vertx vertx;


    public static void main(String[] args) throws UnknownHostException {
        log.info("Booting up the ServiceA...");

        int port = AvailablePortFinder.find(2000, 2010);

        // 1. service options
        ServiceOptions serviceOptions = new ServiceOptions();
        serviceOptions.setAddress(InetAddress.getLocalHost().getHostAddress());
        serviceOptions.setPort(port);
        serviceOptions.setName("Service A");
        serviceOptions.setTags(Arrays.asList("test", "no-production-ready"));
        // no need to set node id since it's being generated within cluster manager.
        // serviceOptions.setId(UUID.randomUUID().toString());

        // 3. consul cluster manager.
        ConsulClusterManager consulClusterManager = new ConsulClusterManager(new ConsulClusterManagerOptions(serviceOptions));

        // 4. vertx
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setEventLoopPoolSize(1);
        vertxOptions.setClusterManager(consulClusterManager);

        Vertx.clusteredVertx(vertxOptions, res -> {
            if (res.succeeded()) {
                log.info("Clustered vertx instance has been successfully created.");
                vertx = res.result();

                ServiceAVerticle verticle = new ServiceAVerticle(port);
                log.info("Deploying ServiceAVerticle ... ");
                vertx.deployVerticle(verticle);
            } else {
                log.info("Clustered vertx instance failed.");
            }
        });
    }

    /**
     * Simple dedicated test verticle.
     */
    private static class ServiceAVerticle extends AbstractVerticle {
        private final int httpPort;

        ServiceAVerticle(int httpPort) {
            this.httpPort = httpPort;
        }

        @Override
        public void start(Future<Void> startFuture) throws Exception {
            log.trace("Staring ServiceAVerticle on port: '{}'", httpPort);
            final HttpServer httpServer = vertx.createHttpServer();
            final Router router = Router.router(vertx);

            router.route("/serviceA*").handler(BodyHandler.create());
            router.get("/serviceA").handler(event -> {
                vertx.eventBus().send("vertx-consul", "ping", replyHandler -> {
                    if (replyHandler.succeeded()) {
                        log.trace(replyHandler.result().body().toString());
                    } else {
                        log.error(replyHandler.cause().toString());
                    }
                });
                event.response().setStatusCode(HttpResponseStatus.OK.code()).end("Working!");
            });

            httpServer.requestHandler(router::accept).listen(httpPort, result -> {
                if (result.succeeded()) {
                    log.trace("ServiceAVerticle has been started.");
                    startFuture.complete();
                } else {
                    log.warn("ServiceAVerticle couldn't get started :(. Details: '{}'", startFuture.cause().getMessage());
                    startFuture.fail(result.cause());
                }
            });
        }
    }
}
