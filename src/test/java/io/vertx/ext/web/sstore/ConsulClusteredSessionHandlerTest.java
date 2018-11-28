package io.vertx.ext.web.sstore;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.spi.cluster.consul.ConsulCluster;
import io.vertx.spi.cluster.consul.ConsulClusterManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.function.Consumer;

public class ConsulClusteredSessionHandlerTest extends ClusteredSessionHandlerTest {

  private final static int DEFAULT_PORT = 9898;
  private static int consulAgentPort = 8500;

  @BeforeClass
  public static void startConsulCluster() {
    consulAgentPort = ConsulCluster.init();
  }

  @AfterClass
  public static void shutDownConsulCluster() {
    ConsulCluster.shutDown();
  }

  @Override
  protected ClusterManager getClusterManager() {
    return new ConsulClusterManager(getConsulClientOptions());
  }

  protected HttpServerOptions getHttpServerOptions() {
    return new HttpServerOptions().setPort(DEFAULT_PORT).setHost("localhost");
  }

  protected HttpClientOptions getHttpClientOptions() {
    return new HttpClientOptions().setDefaultPort(DEFAULT_PORT);
  }

  protected void testRequestBuffer(HttpMethod method, String path, Consumer<HttpClientRequest> requestAction, Consumer<HttpClientResponse> responseAction,
                                   int statusCode, String statusMessage,
                                   Buffer responseBodyBuffer, boolean normalizeLineEndings) throws Exception {
    testRequestBuffer(client, method, DEFAULT_PORT, path, requestAction, responseAction, statusCode, statusMessage, responseBodyBuffer, normalizeLineEndings);
  }

  private ConsulClientOptions getConsulClientOptions() {
    return new ConsulClientOptions()
      .setPort(consulAgentPort)
      .setHost("localhost");
  }
}
