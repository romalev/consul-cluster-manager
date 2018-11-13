//package io.vertx.ext.web.sstore;
//
//import io.vertx.core.http.HttpClientOptions;
//import io.vertx.core.http.HttpServerOptions;
//import io.vertx.core.spi.cluster.ClusterManager;
//import io.vertx.spi.cluster.consul.ConsulClusterManager;
//
//// FIX ME
//public class ConsulClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {
//
//  @Override
//  protected ClusterManager getClusterManager() {
//    return new ConsulClusterManager();
//  }
//
//  protected HttpServerOptions getHttpServerOptions() {
//    return new HttpServerOptions().setPort(9898).setHost("localhost");
//  }
//
//  protected HttpClientOptions getHttpClientOptions() {
//    return new HttpClientOptions().setDefaultPort(9898);
//  }
//}
