package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>A set that attempts to keep all cluster node's data locally cached. This class
 * watches the consul "__vertx.nodes" path, responds to update/create/delete events, pull down the data.
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. </p>
 *
 * @author Roman Levytskyi
 */
public class ClusterNodeSet extends ConsulMap<String, String> {

  private final static Logger log = LoggerFactory.getLogger(ClusterNodeSet.class);

  private final static String NAME = "__vertx.nodes";
  // local cache of all vertx cluster nodes.
  private Set<String> nodes = new HashSet<>();
  private NodeListener nodeListener;
  private boolean active;

  public ClusterNodeSet(CmContext cmContext) {
    super(NAME, cmContext);
    startListening();
  }

  /**
   * Registers node within the cluster (__vertx.nodes map).
   */
  public Future<Void> add(JsonObject details) {
    Future<Void> future = Future.future();
    putConsulValue(
      keyPath(context.getNodeId()),
      details.encode(),
      new KeyValueOptions().setAcquireSession(context.getEphemeralSessionId()))
      .setHandler(asyncResult -> {
        if (asyncResult.failed()) {
          log.error("[" + context.getNodeId() + "]" + " - Failed to put node: " + " to: " + name, asyncResult.cause());
          future.fail(asyncResult.cause());
        } else future.complete();
      });
    return future;
  }

  public Future<Boolean> remove() {
    return deleteConsulValue(keyPath(context.getNodeId()));
  }

  public Future<Boolean> isEmpty() {
    return consulKeys().compose(nodeIds -> Future.succeededFuture(nodeIds.isEmpty()));
  }

  public void setActive(boolean active) {
    this.active = active;
  }


  public List<String> get() {
    return new ArrayList<>(nodes);
  }

  public void nodeListener(NodeListener nodeListener) {
    this.nodeListener = nodeListener;
  }

  @Override
  public synchronized void entryUpdated(EntryEvent event) {
    if (!active) {
      return;
    }
    context.getVertx().executeBlocking(workingThread -> {
      String receivedNodeId = actualKey(event.getEntry().getKey());
      switch (event.getEventType()) {
        case WRITE: {
          boolean added = nodes.add(receivedNodeId);
          if (added) {
            log.trace("[" + context.getNodeId() + "]" + " New node: " + receivedNodeId + " has joined the cluster.");
            if (nodeListener != null) {
              nodeListener.nodeAdded(receivedNodeId);
              log.trace("[" + context.getNodeId() + "]" + " Node: " + receivedNodeId + " has been added to nodeListener.", receivedNodeId);
            }
          }
          break;
        }
        case REMOVE: {
          boolean removed = nodes.remove(receivedNodeId);
          if (removed) {
            log.trace("[" + context.getNodeId() + "]" + " Node: " + receivedNodeId + " has left the cluster.");
            if (nodeListener != null) {
              nodeListener.nodeLeft(receivedNodeId);
              log.trace("[" + context.getNodeId() + "]" + " Node: " + receivedNodeId + " has been removed from nodeListener.");
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
