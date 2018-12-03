package io.vertx.spi.cluster.consul;

import io.vertx.core.*;
import io.vertx.core.impl.TaskQueue;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.*;
import io.vertx.spi.cluster.consul.impl.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Cluster manager that uses Consul. Given implementation is based on vertx consul client.
 * Current restrictions :
 * <p>
 * - The limit on a key's value size of any of the consul maps is 512KB. This is strictly enforced and an HTTP 413 status will be returned to
 * any client that attempts to store more than that limit in a value. It should be noted that the Consul key/value store is not designed to be used as a general purpose database.
 * <p>
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager extends ConsulMap<String, String> implements ClusterManager {

  private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);
  private static final String HA_INFO_MAP_NAME = "__vertx.haInfo";
  private static final String NODES_MAP_NAME = "__vertx.nodes";
  private static final String SERVICE_NAME = "vert.x-cluster-manager";
  private static final String TCP_CHECK_INTERVAL = "10s";

  private final Map<String, Lock> locks = new ConcurrentHashMap<>();
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();
  private final Map<String, Map<?, ?>> syncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMaps = new ConcurrentHashMap<>();

  /*
   * A set that attempts to keep all cluster node's data locally cached. Cluster manager
   * watches the consul "__vertx.nodes" path, responds to update/create/delete events, pull down the data.
   */
  private final Set<String> nodes = new HashSet<>();
  /*
   * We have to lock node joining the cluster and node leaving the cluster operations
   * to stay as much as possible transactionally in sync with consul kv store.
   * Only one node joins the cluster at particular point in time. This is achieved through acquiring a lock.
   * Given lock is held until the node (that got registered itself) receives an appropriate "NODE JOINED" event through consul watch
   * about itself -> lock gets release then. Having this allow us to ensure :
   * - nodeAdded and nodeRemoved are called on the nodeListener in the same order on the same nodes with same node ids.
   * - getNodes() always return same node list when nodeAdded and nodeRemoved are called on the nodeListener.
   * Without locking we'd screw up an entire HA.
   *
   * Note: question was raised in consul google groups: https://groups.google.com/forum/#!topic/consul-tool/A0yJV0EKclw
   * so far no answers.
   */
  private final static String NODE_JOINING_LOCK_NAME = "nodeJoining";
  private final static String NODE_LEAVING_LOCK_NAME = "nodeLeaving";
  private final AtomicReference<Lock> nodeJoiningLock = new AtomicReference<>();
  private final AtomicReference<Lock> nodeLeavingLock = new AtomicReference<>();

  private final TaskQueue taskQueue = new TaskQueue();
  /*
   * Famous CAP theorem takes place here.
   * * CP - we trade consistency against availability,
   * * AP - we trade availability (latency) against consistency.
   * Given cluster management SPI implementation uses internal caching of consul KV stores (pure @{link ConcurrentHashMap} acts as cache and
   * caching is implemented on top consul watches.)
   *
   * Scenario: imagine we have nodes A and B in our cluster. A puts entry X to Consul KV and gets entry X cached locally.
   * B receives ENTRY_ADDED event (which is a X entry) and caches it locally accordingly.
   * Now imagine A removes an entry X from Consul KV and in one nanosecond later (right after remove of entry X has been acknowledged) node B queries entry X
   * and … well it receives an entry X from local cache since local node B cache became inconsistent with Consul KV since it has not yet received and processed EVENT_REMOVED event.
   * In order make local caching strongly consistent with Consul KV we have to check if data that is present in local cache is the same as data that is present in Consul KV.
   *
   * To enable dirty reads (and prefer having better latency) set {@code preferConsistency} flag to FALSE.
   * Otherwise set {@code preferConsistency} flag to TRUE to enable stronger consistency level (i.e. disable internal caching and use consul kv store directly).
   */
  private final boolean preferConsistency;

  private String checkId; // tcp consul check id
  private NetServer tcpServer; // dummy TCP server to receive and acknowledge heart beats messages from consul.
  private JsonObject nodeTcpAddress = new JsonObject(); // node's tcp address.
  private volatile boolean active; // identifies whether cluster manager is active or passive.
  private NodeListener nodeListener;

  /**
   * Creates consul cluster manager instance with specified config.
   * Example:
   * <pre>
   * JsonObject options = new JsonObject()
   *  .put("host", "localhost")
   *  .put("port", consulAgentPort)
   *  .put("preferConsistency", true);
   * </pre>
   * If config key is not specified -> default configuration takes place:
   * host -> localhost; port - 8500; preferConsistency - false
   *
   * @param config holds configuration for consul cluster manager client.
   */
  public ConsulClusterManager(final JsonObject config) {
    super(NODES_MAP_NAME, new ConsulMapContext());
    Objects.requireNonNull(config, "Given cluster manager can't get initialized.");
    mapContext
      .setConsulClientOptions(new ConsulClientOptions(config))
      .setNodeId(UUID.randomUUID().toString());
    this.checkId = mapContext.getNodeId();
    this.preferConsistency = config.containsKey("preferConsistency") ? config.getBoolean("preferConsistency") : false;
  }

  /**
   * Creates consul cluster manager instance with default configurations:
   * host -> localhost; port - 8500; preferConsistency - false
   */
  public ConsulClusterManager() {
    this(new JsonObject());
  }

  @Override
  public void setVertx(Vertx vertx) {
    mapContext.setVertx(vertx);
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
    Future<AsyncMultiMap<K, V>> futureAsyncMultiMap = Future.future();
    AsyncMultiMap asyncMultiMap = asyncMultiMaps.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, preferConsistency, mapContext));
    futureAsyncMultiMap.complete(asyncMultiMap);
    futureAsyncMultiMap.setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
    Future<AsyncMap<K, V>> futureAsyncMap = Future.future();
    AsyncMap asyncMap = asyncMaps.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, mapContext, this));
    futureAsyncMap.complete(asyncMap);
    futureAsyncMap.setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return (Map<K, V>) syncMaps.computeIfAbsent(name, key -> new ConsulSyncMap<>(name, mapContext));
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    mapContext.getVertx().executeBlocking(futureLock -> {
      ConsulLock lock = new ConsulLock(name, checkId, timeout, mapContext);
      boolean lockObtained = false;
      long remaining = timeout;
      do {
        long start = System.nanoTime();
        try {
          lockObtained = lock.tryObtain();
        } catch (VertxException e) {
          // OK continue
        }
        remaining = remaining - MILLISECONDS.convert(System.nanoTime() - start, NANOSECONDS);
      } while (!lockObtained && remaining > 0);
      if (lockObtained) {
        locks.put(name, lock);
        futureLock.complete(lock);
      } else {
        futureLock.fail("Timed out waiting to get lock: " + name);
      }
    }, false, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    Objects.requireNonNull(name);
    Future<Counter> counterFuture = Future.future();
    Counter counter = counters.computeIfAbsent(name, key -> new ConsulCounter(name, mapContext));
    counterFuture.complete(counter);
    counterFuture.setHandler(resultHandler);
  }

  @Override
  public String getNodeID() {
    return mapContext.getNodeId();
  }

  @Override
  public List<String> getNodes() {
    return new ArrayList<>(nodes);
  }

  @Override
  public void nodeListener(NodeListener listener) {
    this.nodeListener = listener;
  }

  /**
   * For new node to join the cluster we perform:
   * <p>
   * - We create {@link ConsulClient} instance and save it to internal {@link ConsulMapContext}
   * <p>
   * - We register consul service in consul agent. Every new consul service and new entry within "__vertx.nodes" represent actual vertx node.
   * Note: every vetx node that joins the cluster IS tagged with NODE_COMMON_TAG = "vertx-clustering".Don't confuse vertx node with consul (native) node - these are completely different things.
   * <p>
   * - We create dummy TCP server to able to receive and acknowledge heart beats messages from consul.
   * <p>
   * - We create TCP check (and get it registered within consul agent) to let consul agent sends heart beats messages to previously mentioned tcp server.
   * This allow consul agent to be aware of what is going on within the cluster: node is active if it acknowledges hear beat message, inactive - otherwise.
   * {@code TCP_CHECK_INTERVAL} holds actual interval for which consul agent will be sending heart beat messages to vert.x nodes.
   * <p>
   * - We create session in consul agent. Session's id is used later on to make consul map entries ephemeral (every entry that gets created with special {@link KeyValueOptions} holding this session id gets automatically deleted
   * from the consul cluster once session gets invalidated).
   * In this cluster manager case session will get invalidated when:
   * <li>health check gets unregistered.</li>
   * <li>health check falls into critical state {@link CheckStatus} - this happens when our dummy TCP server doesn't acknowledge the consul's heartbeat message).</li>
   * <li>session is explicitly destroyed.</li>
   * <p>
   * - TODO : managing nodes.
   */
  @Override
  public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
    log.trace(mapContext.getNodeId() + " is trying to join the cluster.");
    if (!active) {
      active = true;
      mapContext.initConsulClient();
      deregisterFailingTcpChecks()
        .compose(aVoid -> createTcpServer())
        .compose(aVoid -> registerService())
        .compose(aVoid -> registerTcpCheck())
        .compose(aVoid ->
          registerSession("Session for ephemeral keys for: " + mapContext.getNodeId(), checkId)
            .compose(s -> {
              mapContext.setEphemeralSessionId(s);
              return succeededFuture();
            }))
        .compose(aVoid -> {
          startListening(); // start a watch to listen for an updates on _vertx.nodes.
          return clearHaInfoMap();
        })
        .compose(aVoid -> addLocalNode(nodeTcpAddress))
        .setHandler(nodeJoinedEvent -> {
          if (nodeJoinedEvent.succeeded())
            resultHandler.handle(succeededFuture());
          else {
            // undo - if node can't join the cluster then:
            try {
              shutdownTcpServer();
              // this will trigger the remove of all ephemeral entries
              if (Objects.nonNull(mapContext.getEphemeralSessionId()))
                destroySession(mapContext.getEphemeralSessionId());
              deregisterTcpCheck();
              mapContext.close();
            } finally {
              resultHandler.handle(failedFuture(nodeJoinedEvent.cause()));
            }
          }
        });
    } else {
      log.warn(mapContext.getNodeId() + " is NOT active.");
      resultHandler.handle(succeededFuture());
    }
  }

  /**
   * This describers what happens when existing node leaves the cluster.
   */
  @Override
  public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
    log.trace(mapContext.getNodeId() + " is trying to leave the cluster.");
    if (active) {
      active = false;
      // forcibly release all lock being held by node.
      locks.values().forEach(Lock::release);
      removeLocalNode()
        .compose(aVoid -> destroySession(mapContext.getEphemeralSessionId()))
        .compose(aVoid -> deregisterTcpCheck())
        .compose(aVoid -> shutdownTcpServer())
        .compose(aVoid -> {
          mapContext.close();
          log.trace("[" + mapContext.getNodeId() + "] has left the cluster.");
          return Future.<Void>succeededFuture();
        })
        .setHandler(resultHandler);
    } else {
      log.warn(mapContext.getNodeId() + "' is NOT active.");
      resultHandler.handle(Future.succeededFuture());
    }
  }

  @Override
  public boolean isActive() {
    return active;
  }

  /**
   * Adds local node to the cluster (new entry gets created within {@code NODES_MAP_NAME} where key is node id).
   *
   * @param details - IP address of node.
   */
  private Future<Void> addLocalNode(JsonObject details) {
    Future<Lock> lockFuture = Future.future();
    getLockWithTimeout(NODE_JOINING_LOCK_NAME, 5000, lockFuture.completer());
    return lockFuture.compose(aLock -> {
      nodeJoiningLock.set(aLock);
      return putPlainValue(
        keyPath(mapContext.getNodeId()),
        details.encode(),
        new KeyValueOptions().setAcquireSession(mapContext.getEphemeralSessionId())
      );
    }).compose(aBoolean -> {
      if (aBoolean) return Future.succeededFuture();
      else {
        nodeJoiningLock.get().release();
        return Future.failedFuture("Node: " + mapContext.getNodeId() + "failed to join the cluster.");
      }
    });
  }

  /**
   * Removes local node from the cluster (existing entry gets removed from {@code NODES_MAP_NAME}).
   */
  private Future<Void> removeLocalNode() {
    Future<Lock> lockFuture = Future.future();
    getLockWithTimeout(NODE_LEAVING_LOCK_NAME, 5000, lockFuture.completer());
    return lockFuture.compose(aLock -> {
      nodeLeavingLock.set(aLock);
      return deleteValueByKeyPath(keyPath(mapContext.getNodeId()));
    }).compose(aBoolean -> {
      if (aBoolean) return Future.succeededFuture();
      else {
        nodeLeavingLock.get().release();
        return Future.failedFuture("Node: " + mapContext.getNodeId() + "failed to leave the cluster.");
      }
    });
  }

  /**
   * Checks cluster on emptiness.
   */
  private Future<Boolean> isClusterEmpty() {
    return plainKeys().compose(clusterNodes -> Future.succeededFuture(clusterNodes.isEmpty()));
  }

  /**
   * Clears out an entire HA_INFO map in case cluster is empty.
   */
  private Future<Void> clearHaInfoMap() {
    return isClusterEmpty().compose(clusterEmpty -> {
      if (clusterEmpty) {
        Future<Void> clearHaInfoFuture = Future.future();
        ((ConsulSyncMap) getSyncMap(HA_INFO_MAP_NAME)).clear(handler -> clearHaInfoFuture.complete());
        return clearHaInfoFuture;
      } else {
        return Future.succeededFuture();
      }
    });
  }

  @Override
  public void entryUpdated(EntryEvent event) {
    String receivedNodeId = actualKey(event.getEntry().getKey());
    switch (event.getEventType()) {
      case WRITE: {
        boolean added = nodes.add(receivedNodeId);
        if (added) {
          log.trace("[" + mapContext.getNodeId() + "]" + " New node: " + receivedNodeId + " has joined the cluster.");
        }
        if (receivedNodeId.equals(mapContext.getNodeId())) {
          nodeJoiningLock.get().release();
        }
        if (nodeListener != null && active) {
          VertxInternal vertxInternal = (VertxInternal) mapContext.getVertx();
          vertxInternal.getOrCreateContext().executeBlocking(runOnWorkingPool -> {
            nodeListener.nodeAdded(receivedNodeId);
            runOnWorkingPool.complete();
          }, taskQueue, res -> log.trace("[" + mapContext.getNodeId() + "]" + " Node: " + receivedNodeId + " has been added to nodeListener.", receivedNodeId));
        }
        break;
      }
      case REMOVE: {
        boolean removed = nodes.remove(receivedNodeId);
        if (removed) {
          log.trace("[" + mapContext.getNodeId() + "]" + " Node: " + receivedNodeId + " has left the cluster.");
        }

        if (receivedNodeId.equals(mapContext.getNodeId())) {
          nodeLeavingLock.get().release();
        }
        if (nodeListener != null && active) {
          VertxInternal vertxInternal = (VertxInternal) mapContext.getVertx();
          vertxInternal.getOrCreateContext().executeBlocking(runOnWorkingPool -> {
            nodeListener.nodeLeft(receivedNodeId);
            runOnWorkingPool.complete();
          }, taskQueue, res -> log.trace("[" + mapContext.getNodeId() + "]" + " Node: " + receivedNodeId + " has been removed from nodeListener."));
        }
        break;
      }
    }
  }

  /**
   * Creates simple tcp server used to receive heart beat messages from consul agent and acknowledge them.
   */
  private Future<Void> createTcpServer() {
    Future<Void> future = Future.future();
    try {
      nodeTcpAddress.put("host", InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      log.error(e);
      future.fail(e);
    }
    tcpServer = mapContext.getVertx().createNetServer(new NetServerOptions(nodeTcpAddress));
    tcpServer.connectHandler(event -> {
    }); // node's tcp server acknowledges consul's heartbeat message.
    tcpServer.listen(listenEvent -> {
      if (listenEvent.succeeded()) {
        nodeTcpAddress.put("port", listenEvent.result().actualPort());
        future.complete();
      } else future.fail(listenEvent.cause());
    });
    return future;
  }

  /**
   * Shut downs CM tcp server.
   */
  private Future<Void> shutdownTcpServer() {
    Future<Void> future = Future.future();
    if (tcpServer != null) tcpServer.close(future.completer());
    else future.complete();
    return future;
  }

  /**
   * Registers central @{code SERVICE_NAME} service that is dedicated to vert.x cluster management.
   *
   * @return {@link Future} holding the result.
   */
  private Future<Void> registerService() {
    Future<Void> future = Future.future();
    ServiceOptions serviceOptions = new ServiceOptions();
    serviceOptions.setName(SERVICE_NAME);
    serviceOptions.setTags(Collections.singletonList("vertx-clustering"));
    serviceOptions.setId(SERVICE_NAME);

    mapContext.getConsulClient().registerService(serviceOptions, asyncResult -> {
      if (asyncResult.failed()) {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to register node's service.", asyncResult.cause());
        future.fail(asyncResult.cause());
      } else future.complete();
    });
    return future;
  }

  /**
   * Register tcp check (dedicated to vert.x node) in consul.
   * Gets the node's tcp check registered within consul.
   * These checks make an TCP connection attempt every Interval (in our case this {@code TCP_CHECK_INTERVAL}) to the specified IP/hostname and port.
   * The status of the service depends on whether the connection attempt is successful (ie - the port is currently accepting connections).
   * If the connection is accepted, the status is success, otherwise the status is critical.
   * In the case of a hostname that resolves to both IPv4 and IPv6 addresses, an attempt will be made to both addresses,
   * and the first successful connection attempt will result in a successful check.
   *
   * @return {@link Future} holding the result.
   */
  private Future<Void> registerTcpCheck() {
    Future<Void> future = Future.future();
    CheckOptions checkOptions = new CheckOptions()
      .setName(checkId)
      .setNotes("This check is dedicated to service with id: " + mapContext.getNodeId())
      .setId(checkId)
      .setTcp(nodeTcpAddress.getString("host") + ":" + nodeTcpAddress.getInteger("port"))
      .setServiceId(SERVICE_NAME)
      .setInterval(TCP_CHECK_INTERVAL)
      .setStatus(CheckStatus.PASSING);
    mapContext.getConsulClient().registerCheck(checkOptions, result -> {
      if (result.failed()) {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to register check: " + checkOptions.getId(), result.cause());
        future.fail(result.cause());
      } else future.complete();
    });
    return future;
  }

  /**
   * Deregisters vert.x node's tcp check.
   *
   * @return {@link Future} holding the result.
   */
  private Future<Void> deregisterTcpCheck() {
    Future<Void> future = Future.future();
    mapContext.getConsulClient().deregisterCheck(checkId, resultHandler -> {
      if (resultHandler.succeeded()) future.complete();
      else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to deregister check: " + checkId, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Tries to deregister all failing tcp checks from {@code SERVICE_NAME}.
   */
  private Future<Void> deregisterFailingTcpChecks() {
    Future<CheckList> checkListFuture = Future.future();
    mapContext.getConsulClient().healthChecks(SERVICE_NAME, checkListFuture.completer());
    return checkListFuture.compose(checkList -> {
      List<Future> futures = new ArrayList<>();
      checkList.getList().forEach(check -> {
        if (check.getStatus() == CheckStatus.CRITICAL) {
          Future<Void> future = Future.future();
          mapContext.getConsulClient().deregisterCheck(check.getId(), future.completer());
          futures.add(future);
        }
      });
      return CompositeFuture.all(futures).compose(compositeFuture -> Future.succeededFuture());
    });
  }
}
