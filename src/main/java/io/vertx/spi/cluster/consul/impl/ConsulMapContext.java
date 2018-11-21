package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Vertx;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.Watch;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Consul map mapContext.
 *
 * @author Roman Levytskyi
 */
public final class ConsulMapContext implements AutoCloseable {

  private String nodeId;
  private Vertx vertx;
  private ConsulClient consulClient;
  private ConsulClientOptions consulClientOptions;
  private String ephemeralSessionId; // consul session id used to make map entries ephemeral.
  // TODO: do we really need ConcurrentLinkedQueue?
  private Queue<Watch<KeyValueList>> watchQueue = new ConcurrentLinkedQueue<>();

  public ConsulMapContext setVertx(Vertx vertx) {
    Objects.requireNonNull(vertx);
    this.vertx = vertx;
    return this;
  }

  public ConsulMapContext initConsulClient() {
    Objects.requireNonNull(vertx);
    Objects.requireNonNull(consulClientOptions);
    this.consulClient = ConsulClient.create(vertx, consulClientOptions);
    return this;
  }

  public ConsulMapContext reInitConsulClient() {
    Objects.requireNonNull(consulClient);
    try {
      consulClient.close();
    } catch (IllegalStateException e) {

    }
    return initConsulClient();
  }

  public ConsulMapContext setNodeId(String nodeId) {
    Objects.requireNonNull(nodeId);
    this.nodeId = nodeId;
    return this;
  }

  public ConsulMapContext setConsulClientOptions(ConsulClientOptions consulClientOptions) {
    Objects.requireNonNull(consulClientOptions);
    this.consulClientOptions = consulClientOptions;
    return this;
  }

  public ConsulMapContext setEphemeralSessionId(String sessionId) {
    Objects.requireNonNull(sessionId);
    this.ephemeralSessionId = sessionId;
    return this;
  }

  public String getNodeId() {
    Objects.requireNonNull(nodeId);
    return nodeId;
  }

  public Vertx getVertx() {
    Objects.requireNonNull(vertx);
    return vertx;
  }

  public ConsulClient getConsulClient() {
    Objects.requireNonNull(consulClient);
    return consulClient;
  }

  public ConsulClientOptions getConsulClientOptions() {
    Objects.requireNonNull(consulClientOptions);
    return consulClientOptions;
  }

  public String getEphemeralSessionId() {
    Objects.requireNonNull(ephemeralSessionId);
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

  @Override
  public void close() {
    try {
      consulClient.close();
    } catch (IllegalStateException e) {

    }
    watchQueue.forEach(Watch::stop);
  }
}
