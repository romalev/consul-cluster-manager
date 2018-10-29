package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueList;
import io.vertx.ext.consul.KeyValueOptions;
import io.vertx.ext.consul.Watch;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>A set that attempts to keep all cluster node's data locally cached. This class
 * watches the consul "__vertx.nodes" path, responds to update/create/delete events, pull down the data.
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. </p>
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterNodeSet extends ConsulMap<String, String> implements ConsulKvListener {

  private final static Logger log = LoggerFactory.getLogger(ConsulClusterNodeSet.class);

  // local cache of all vertx cluster nodes.
  private Set<String> nodes = new HashSet<>();
  private final String sessionId;
  private NodeListener nodeListener;
  private CacheManager cM;

  public ConsulClusterNodeSet(String nodeId, Vertx vertx, ConsulClient consulClient, String sessionId, CacheManager cM) {
    super("__vertx.nodes", nodeId, vertx, consulClient);
    this.sessionId = sessionId;
    this.cM = cM;
  }

  /**
   * Discovers nodes that are currently available within the cluster.
   *
   * @return completed future if nodes (consul services) have been successfully fetched from consul cluster,
   * failed future - otherwise.
   */
  public Future<Void> discover() {
    return consulKeys()
      .compose(list -> {
        if (list == null) return Future.succeededFuture();
        nodes = list.stream().map(this::actualKey).collect(Collectors.toSet());
        log.trace("[" + nodeId + "]" + " - Available nodes within the cluster: " + nodes);
        return Future.succeededFuture();
      });
  }

  /**
   * Registers node within the cluster (__vertx.nodes map).
   */
  public Future<Void> add(JsonObject details) {
    Future<Void> future = Future.future();
    putValue(nodeId, details.encode(), new KeyValueOptions().setAcquireSession(sessionId)).setHandler(asyncResult -> {
      if (asyncResult.failed()) {
        log.error("[" + nodeId + "]" + " - Failed to put node: " + " to: " + name, asyncResult.cause());
        future.fail(asyncResult.cause());
      } else future.complete();
    });
    return future;
  }

  public List<String> get() {
    return new ArrayList<>(nodes);
  }

  public void nodeListener(NodeListener nodeListener) {
    this.nodeListener = nodeListener;
    Watch<KeyValueList> watch = cM.createAndGetMapWatch(name);
    watch.setHandler(kvWatchHandler()).start();
  }

  @Override
  public void entryUpdated(EntryEvent event) {
    vertx.executeBlocking(workingThread -> {
      String receivedNodeId = actualKey(event.getEntry().getKey());
      switch (event.getEventType()) {
        case WRITE: {
          boolean added = nodes.add(receivedNodeId);
          if (added) {
            log.trace("[" + nodeId + "]" + " New node: " + receivedNodeId + " has joined the cluster.");
            if (nodeListener != null) {
              nodeListener.nodeAdded(receivedNodeId);
              log.trace("[" + nodeId + "]" + " Node: " + receivedNodeId + " has been added to nodeListener.", receivedNodeId);
            }
          }
          break;
        }
        case REMOVE: {
          boolean removed = nodes.remove(receivedNodeId);
          if (removed) {
            log.trace("[" + nodeId + "]" + " Node: " + receivedNodeId + " has left the cluster.");
            if (nodeListener != null) {
              nodeListener.nodeLeft(receivedNodeId);
              log.trace("[" + nodeId + "]" + " Node: " + receivedNodeId + " has been removed from nodeListener.");
            }
          }
          break;
        }
      }
      workingThread.complete();
    }, result -> {
    });
  }
}
