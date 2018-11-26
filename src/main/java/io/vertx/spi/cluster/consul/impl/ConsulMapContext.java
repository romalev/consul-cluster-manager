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
    this.vertx = Objects.requireNonNull(vertx);
    return this;
  }

  public ConsulMapContext initConsulClient() {
    this.consulClient = ConsulClient.create(Objects.requireNonNull(vertx), Objects.requireNonNull(consulClientOptions));
    return this;
  }

  public ConsulMapContext setNodeId(String nodeId) {
    this.nodeId = Objects.requireNonNull(nodeId);
    return this;
  }

  public ConsulMapContext setConsulClientOptions(ConsulClientOptions consulClientOptions) {
    this.consulClientOptions = Objects.requireNonNull(consulClientOptions);
    return this;
  }

  public ConsulMapContext setEphemeralSessionId(String sessionId) {
    this.ephemeralSessionId = Objects.requireNonNull(sessionId);
    return this;
  }

  public String getNodeId() {
    return Objects.requireNonNull(nodeId);
  }

  public Vertx getVertx() {
    return Objects.requireNonNull(vertx);
  }

  public ConsulClient getConsulClient() {
    return Objects.requireNonNull(consulClient);
  }

  public ConsulClientOptions getConsulClientOptions() {
    return Objects.requireNonNull(consulClientOptions);
  }

  public String getEphemeralSessionId() {
    return Objects.requireNonNull(ephemeralSessionId);
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
