package io.vertx.spi.cluster.consul.example;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.consul.ConsulClusterManager;


public class SecondVerticle extends AbstractVerticle {
  public static void main(String... args) {
    ConsulClusterManager clusterManager = new ConsulClusterManager();
    Vertx.clusteredVertx(new VertxOptions().setHAEnabled(true).setClusterManager(clusterManager), vertx ->
      vertx.result().deployVerticle(SecondVerticle.class.getName(), new DeploymentOptions().setHa(true))
    );
  }

  @Override
  public void start() throws Exception {
    vertx.createHttpServer().requestHandler(req ->
      req.response().end("Request served by " + SecondVerticle.class.getName() + "\n")
    ).listen(9090);
  }
}
