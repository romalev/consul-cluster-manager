package io.vertx.spi.cluster.consul.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;

import java.lang.management.ManagementFactory;

public class HaVerticle extends AbstractVerticle {
  /**
   * The entry point of application.
   *
   * @param args the input arguments
   */
  public static void main(String... args) {
    ConsulClusterManager consulClusterManager = new ConsulClusterManager();
    Vertx.clusteredVertx(new VertxOptions().setHAEnabled(true).setClusterManager(consulClusterManager), vertx ->
      vertx.result().deployVerticle(HaVerticle.class.getName(), new DeploymentOptions().setHa(true))
    );
  }


  @Override
  public void start() throws Exception {
    vertx.createHttpServer().requestHandler(req ->
      req.response().end("Request served by " +
        ManagementFactory.getRuntimeMXBean().getName() + "\n")
    ).listen(8181);
  }
}
