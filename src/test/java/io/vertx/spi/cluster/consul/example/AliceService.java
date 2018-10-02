package io.vertx.spi.cluster.consul.example;

import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.spi.cluster.consul.ConsulClusterManager;

public class AliceService {

    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final Logger log = LoggerFactory.getLogger(AliceService.class);
    private static Vertx vertx;


    public static void main(String[] args) {
        log.info("Booting up the ServiceA...");

        //ZookeeperClusterManager zookeeperClusterManager = new ZookeeperClusterManager();
        ConsulClusterManager consulClusterManager = new ConsulClusterManager();
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setHAEnabled(true);
        vertxOptions.setHAGroup("test-ha-group");
        vertxOptions.setClusterManager(consulClusterManager);
        Vertx.clusteredVertx(vertxOptions, res -> {
            if (res.succeeded()) {
                log.info("Clustered vertx instance has been successfully created.");
                vertx = res.result();

                ServiceAVerticle verticle = new ServiceAVerticle();
                log.info("Deploying ServiceAVerticle ... ");
                vertx.deployVerticle(verticle, event -> {
                    if (event.succeeded()) {
                        log.info("Vertcile: '{}' deployed.", event.result());
                        // countDownLatch.countDown();
                    }
                });
            } else {
                log.info("Clustered vertx instance failed.");
            }
        });
    }

    /**
     * Simple dedicated test verticle.
     */
    private static class ServiceAVerticle extends AbstractVerticle {
        @Override
        public void start(Future<Void> startFuture) throws Exception {
            log.trace("Staring ServiceAVerticle...");
            final HttpServer httpServer = vertx.createHttpServer();
            final Router router = Router.router(vertx);

            router.route("/serviceA*").handler(BodyHandler.create());
            router.get("/serviceA/vertxclose").handler(event -> {
                vertx.close();
                event.response().setStatusCode(HttpResponseStatus.OK.code()).end("Vertx closed.");
            });

            router.get("/serviceA").handler(event -> {
//                vertx.sharedData().getAsyncMap("custom", result -> {
//                    if (result.succeeded()) {
//                        AsyncMap<Object, Object> asyncMap = result.result();
//                        asyncMap.put("Roman", "Lev", 20, completionHandler -> {
//
//                        });
//                    } else {
//                        log.error("Can't get custom map due to: {}", result.cause().toString());
//                    }
//                });

                vertx.eventBus().send("vertx-consul", "ping", replyHandler -> {
                    if (replyHandler.succeeded()) {
                        log.trace(replyHandler.result().body().toString());
                    } else {
                        log.error(replyHandler.cause().toString());
                    }
                });
                event.response().setStatusCode(HttpResponseStatus.OK.code()).end("Working!");
            });

            httpServer.requestHandler(router::accept).listen(Utils.findPort(2000, 2010), result -> {
                if (result.succeeded()) {
                    log.trace("ServiceAVerticle has been started on port: '{}'", result.result().actualPort());
                    startFuture.complete();
                } else {
                    log.warn("ServiceAVerticle couldn't get started :(. Details: '{}'", startFuture.cause().getMessage());
                    startFuture.fail(result.cause());
                }
            });
        }
    }
}
