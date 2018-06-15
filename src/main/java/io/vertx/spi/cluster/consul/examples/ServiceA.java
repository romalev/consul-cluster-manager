package io.vertx.spi.cluster.consul.examples;


import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.ServiceOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import io.vertx.spi.cluster.consul.impl.AvailablePortFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class ServiceA {

    private static final Logger log = LoggerFactory.getLogger(ServiceA.class);

    private static Vertx vertx;

    public static void main(String[] args) throws UnknownHostException {
        log.info("Booting up the ServiceA...");
        // 1. service options
        ServiceOptions serviceOptions = new ServiceOptions();
        serviceOptions.setAddress(InetAddress.getLocalHost().getHostAddress());
        serviceOptions.setPort(AvailablePortFinder.find(2000, 2010));
        serviceOptions.setName("Service A");
        serviceOptions.setId(UUID.randomUUID().toString());

        // 2. consul agent options.
        ConsulClientOptions options = new ConsulClientOptions();

        // 3. consul cluster manager.
        ConsulClusterManager consulClusterManager = new ConsulClusterManager(serviceOptions, options);

        // 4. vertx
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setClusterManager(consulClusterManager);

        Vertx.clusteredVertx(vertxOptions, res -> {
            if (res.succeeded()) {
                log.info("Clustered vertx instance has been successfully created.");
                vertx = res.result();
            } else {
                log.info("Clustered vertx instance failed.");
            }
        });
    }

}
