package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Cluster manager context.
 *
 * @author Roman Levytskyi
 */
public final class CmContext {

  private String nodeId;
  private Vertx vertx;
  private ConsulClient consulClient;
  private ConsulClientOptions consulClientOptions;
  private String ephemeralSessionId; // consul session id used to make map entries ephemeral.
  private Queue<Watch<KeyValueList>> watchQueue = new ConcurrentLinkedDeque<>();

  public CmContext setVertx(Vertx vertx) {
    Objects.requireNonNull(vertx);
    this.vertx = vertx;
    return this;
  }

  public CmContext setConsulClient(ConsulClient consulClient) {
    Objects.requireNonNull(consulClient);
    this.consulClient = consulClient;
    return this;
  }

  public CmContext setNodeId(String nodeId) {
    Objects.requireNonNull(nodeId);
    this.nodeId = nodeId;
    return this;
  }

  public CmContext setConsulClientOptions(ConsulClientOptions consulClientOptions) {
    Objects.requireNonNull(consulClientOptions);
    this.consulClientOptions = consulClientOptions;
    return this;
  }

  public CmContext setEphemeralSessionId(String sessionId) {
    Objects.requireNonNull(sessionId);
    this.ephemeralSessionId = sessionId;
    return this;
  }

  public String getNodeId() {
    return nodeId;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public ConsulClient getConsulClient() {
    return consulClient;
  }

  public ConsulClientOptions getConsulClientOptions() {
    return consulClientOptions;
  }

  public String getEphemeralSessionId() {
    return ephemeralSessionId;
  }

  public Watch<KeyValueList> createAndGetWatch(String name) {
    Objects.requireNonNull(name);
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(consulClientOptions);
    Watch<KeyValueList> watch = Watch.keyPrefix(name, vertx, consulClientOptions);
    watchQueue.add(watch);
    return watch;
  }

  public void close() {
    watchQueue.forEach(Watch::stop);
  }
}
