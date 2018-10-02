package io.vertx.spi.cluster.consul.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

public class BobService {

    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final Logger log = LoggerFactory.getLogger(BobService.class);
    private static Vertx vertx;


    public static void main(String[] args) throws UnknownHostException {
        log.info("Booting up the Service B...");

        int port = Utils.findPort(2000, 2010);

        // 1. service options
        ServiceOptions serviceOptions = new ServiceOptions();
        serviceOptions.setAddress(InetAddress.getLocalHost().getHostAddress());
        serviceOptions.setPort(port);
        serviceOptions.setName("Service B");
        serviceOptions.setTags(Arrays.asList("test", "no-production-ready"));
        // no need to set node id since it's being generated within cluster manager.
        // serviceOptions.setId(UUID.randomUUID().toString());

        // 3. consul cluster manager.
        ConsulClusterManager consulClusterManager = new ConsulClusterManager();

        // 4. vertx
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setHAEnabled(true);
        vertxOptions.setHAGroup("test-ha-group");
        vertxOptions.setClusterManager(consulClusterManager);

        Vertx.clusteredVertx(vertxOptions, res -> {
            if (res.succeeded()) {
                log.info("Clustered vertx instance has been successfully created.");
                vertx = res.result();

                ServiceBVerticle verticle = new ServiceBVerticle();
                log.info("Deploying ServiceBVerticle ... ");
                vertx.deployVerticle(verticle);
            } else {
                log.info("Clustered vertx instance failed.");
            }
        });
    }

    /**
     * Simple dedicated test verticle.
     */
    private static class ServiceBVerticle extends AbstractVerticle {
        @Override
        public void start(Future<Void> startFuture) throws Exception {
            vertx.eventBus().consumer("vertx-consul", event -> {
                log.trace("Command was received: '{}'", event.body());
                event.reply("pong");
            });
            startFuture.complete();
        }
    }
}
