//package io.vertx.core.eventbus;
//
//import io.vertx.core.spi.cluster.ClusterManager;
//import io.vertx.ext.consul.ConsulClientOptions;
//import io.vertx.spi.cluster.consul.ConsulCluster;
//import io.vertx.spi.cluster.consul.ConsulClusterManager;
//import org.junit.AfterClass;
//import org.junit.BeforeClass;
//
///**
// * ./gradlew test --info --tests io.vertx.core.eventbus.ConsulFaultToleranceTest
// */
//public class ConsulFaultToleranceTest extends FaultToleranceTest {
//
//  private static int port;
//
//  @BeforeClass
//  public static void startConsulCluster() {
//    port = ConsulCluster.init();
//  }
//
//  @AfterClass
//  public static void shutDownConsulCluster() {
//    ConsulCluster.shutDown();
//  }
//
//  @Override
//  protected ClusterManager getClusterManager() {
//    ConsulClientOptions options = new ConsulClientOptions()
//      .setPort(port)
//      .setHost("localhost");
//    return new ConsulClusterManager(options);
//  }
//
//}
