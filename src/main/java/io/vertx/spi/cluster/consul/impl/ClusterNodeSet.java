package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
public class ClusterNodeSet extends ConsulMap<String, String> {

  private final static Logger log = LoggerFactory.getLogger(ClusterNodeSet.class);

  private final static String NAME = "__vertx.nodes";
  private final static long timeout = 30_000;

  /*
   * TODO: Update why we need this!
   */
  private final CountDownLatch nodeAddedLatch = new CountDownLatch(1);
  private final CountDownLatch nodeRemovedLatch = new CountDownLatch(1);
  private final TaskQueue taskQueue = new TaskQueue();

  private final Set<String> nodes; // local cache of all vertx cluster nodes.
  private NodeListener nodeListener;
  private boolean isClusterManagerActive;


  public ClusterNodeSet(ConfigContext configContext) {
    super(NAME, configContext);
    nodes = completeAndGet(
      consulKeys().compose(keys -> Future.succeededFuture(keys.stream().map(this::actualKey).collect(Collectors.toSet()))),
      timeout);
    startListening();
  }

  /**
   * Registers node within the cluster (__vertx.nodes map).
   */
  public void add(JsonObject details) throws InterruptedException {
    completeAndGet(putConsulValue(
      keyPath(context.getNodeId()), details.encode(), new KeyValueOptions().setAcquireSession(context.getEphemeralSessionId())
    ), timeout);
    nodeAddedLatch.await(20, TimeUnit.SECONDS);
  }

  public void remove() throws InterruptedException {
    completeAndGet(deleteConsulValue(keyPath(context.getNodeId())), timeout);
    nodeAddedLatch.await(20, TimeUnit.SECONDS);
  }

  public boolean isEmpty() {
    return completeAndGet(consulKeys().compose(nodeIds -> Future.succeededFuture(nodeIds.isEmpty())), timeout);
  }

  public void setClusterManagerActive(boolean clusterManagerActive) {
    this.isClusterManagerActive = clusterManagerActive;
  }


  public List<String> get() {
    log.trace("[" + context.getNodeId() + "]: Nodes are: " + nodes);
    return new ArrayList<>(nodes);
  }

  public void nodeListener(NodeListener nodeListener) {
    this.nodeListener = nodeListener;
  }

  @Override
  public void entryUpdated(EntryEvent event) {
    String receivedNodeId = actualKey(event.getEntry().getKey());
    switch (event.getEventType()) {
      case WRITE: {
        boolean added = nodes.add(receivedNodeId);
        if (added) {
          log.trace("[" + context.getNodeId() + "]" + " New node: " + receivedNodeId + " has joined the cluster.");
        }
        if (nodeAddedLatch.getCount() == 1 && receivedNodeId.equals(context.getNodeId())) {
          nodeAddedLatch.countDown();
        }
        if (nodeListener != null && isClusterManagerActive) {
          VertxInternal vertxInternal = (VertxInternal) context.getVertx();
          vertxInternal.getOrCreateContext().executeBlocking(runOnWorkingPool -> {
            nodeListener.nodeAdded(receivedNodeId);
            runOnWorkingPool.complete();
          }, taskQueue, res -> log.trace("[" + context.getNodeId() + "]" + " Node: " + receivedNodeId + " has been added to nodeListener.", receivedNodeId));
        }
        break;
      }
      case REMOVE: {
        boolean removed = nodes.remove(receivedNodeId);
        if (removed) {
          log.trace("[" + context.getNodeId() + "]" + " Node: " + receivedNodeId + " has left the cluster.");
        }

        if (nodeRemovedLatch.getCount() == 1 && receivedNodeId.equals(context.getNodeId())) {
          nodeRemovedLatch.countDown();
        }
        if (nodeListener != null && isClusterManagerActive) {
          VertxInternal vertxInternal = (VertxInternal) context.getVertx();
          vertxInternal.getOrCreateContext().executeBlocking(runOnWorkingPool -> {
            nodeListener.nodeLeft(receivedNodeId);
            runOnWorkingPool.complete();
          }, taskQueue, res -> log.trace("[" + context.getNodeId() + "]" + " Node: " + receivedNodeId + " has been removed from nodeListener."));
        }
        break;
      }
    }
  }
}
