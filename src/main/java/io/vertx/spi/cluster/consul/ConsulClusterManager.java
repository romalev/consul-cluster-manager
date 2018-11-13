package io.vertx.spi.cluster.consul;

import io.vertx.core.*;
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
 * - TTL value (on entries) must be between 10s and 86400s currently. [Invalidation-time is twice the TTL time](https://github.com/hashicorp/consul/issues/1172)
 * this means actual time when ttl entry gets removed (expired) is doubled to what you will specify as a ttl.
 * <p>
 * TODO : do we really need to keep the service registered.
 * TODO: For ha to work correctly verify:
 * - do we need to keep the node id that belongs to the same node?
 * - order in which watcher accepts updates from consul kv.
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager implements ClusterManager {

  private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);
  private static final String HA_INFO = "__vertx.haInfo";
  private static final String TCP_CHECK_INTERVAL = "10s";

  private final Map<String, Lock> locks = new ConcurrentHashMap<>();
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();
  private final Map<String, Map<?, ?>> syncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMaps = new ConcurrentHashMap<>();
  private final CmContext cmContext = new CmContext();
  private ClusterNodeSet nodeSet;

  private String checkId; // tcp consul check id
  private JsonObject nodeTcpAddress = new JsonObject();
  private NetServer netServer; // dummy TCP server to receive and acknowledge heart beats messages from consul.
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
  private boolean preferConsistency = false;

  private volatile boolean active;

  public ConsulClusterManager(final ConsulClientOptions options) {
    Objects.requireNonNull(options, "Consul client options can't be null");
    cmContext
      .setConsulClientOptions(options)
      .setNodeId(UUID.randomUUID().toString());
    this.checkId = cmContext.getNodeId();
  }

  public ConsulClusterManager() {
    cmContext
      .setConsulClientOptions(new ConsulClientOptions())
      .setNodeId(UUID.randomUUID().toString());
    this.checkId = cmContext.getNodeId();
  }

  public ConsulClusterManager(final ConsulClientOptions options, final boolean preferConsistency) {
    Objects.requireNonNull(options, "Consul client options can't be null");
    cmContext.setConsulClientOptions(options)
      .setNodeId(UUID.randomUUID().toString());
    this.checkId = cmContext.getNodeId();
    this.preferConsistency = preferConsistency;
  }

  @Override
  public void setVertx(Vertx vertx) {
    cmContext.setVertx(vertx);
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
    Future<AsyncMultiMap<K, V>> futureAsyncMultiMap = Future.future();
    AsyncMultiMap asyncMultiMap = asyncMultiMaps.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, preferConsistency, cmContext));
    futureAsyncMultiMap.complete(asyncMultiMap);
    futureAsyncMultiMap.setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
    Future<AsyncMap<K, V>> futureAsyncMap = Future.future();
    AsyncMap asyncMap = asyncMaps.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, cmContext));
    futureAsyncMap.complete(asyncMap);
    futureAsyncMap.setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return (Map<K, V>) syncMaps.computeIfAbsent(name, key -> new ConsulSyncMap<>(name, cmContext));
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    cmContext.getVertx().executeBlocking(futureLock -> {
      ConsulLock lock = new ConsulLock(name, checkId, timeout, cmContext);
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
        throw new VertxException("Timed out waiting to get lock " + name);
      }
    }, false, resultHandler);
  }

  @Override
  public void getCounter(String name, Handler<AsyncResult<Counter>> resultHandler) {
    Objects.requireNonNull(name);
    Future<Counter> counterFuture = Future.future();
    Counter counter = counters.computeIfAbsent(name, key -> new ConsulCounter(name, cmContext));
    counterFuture.complete(counter);
    counterFuture.setHandler(resultHandler);
  }

  @Override
  public String getNodeID() {
    return cmContext.getNodeId();
  }

  @Override
  public List<String> getNodes() {
    return nodeSet.get();
  }

  @Override
  public void nodeListener(NodeListener listener) {
    nodeSet.nodeListener(listener);
  }

  /**
   * For new node to join the cluster we perform:
   * <p>
   * - We create {@link ConsulClient} instance and save it to internal {@link CmContext}
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
   * - We create {@link ClusterNodeSet} instance. TODO:
   */
  @Override
  public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
    Future<Void> future = Future.future();
    log.trace(cmContext.getNodeId() + " is trying to join the cluster.");
    if (!active) {
      active = true;
      cmContext.setConsulClient(ConsulClient.create(cmContext.getVertx(), cmContext.getConsulClientOptions()));
      createTcpServer()
        .compose(aVoid -> registerService())
        .compose(aVoid -> registerTcpCheck())
        .compose(aVoid ->
          registerSession("Session for ephemeral keys for: " + cmContext.getNodeId())
            .compose(s -> {
              cmContext.setEphemeralSessionId(s);
              return succeededFuture();
            }))
        .compose(aVoid -> {
          nodeSet = new ClusterNodeSet(cmContext);
          nodeSet.setActive(active);
          return succeededFuture();
        })
        .compose(aVoid -> nodeSet.isEmpty())
        .compose(isEmpty -> {
          if (isEmpty) {
            Future<Void> cleanFuture = Future.future();
            log.trace("Clearing up HA_INFO since the cluster is empty");
            ((ConsulSyncMap) getSyncMap(HA_INFO)).clear(cleanFuture.completer());
            return cleanFuture;
          } else return succeededFuture();
        })
        .compose(aVoid -> nodeSet.add(nodeTcpAddress))
        .setHandler(nodeJoinedEvent -> {
          if (nodeJoinedEvent.succeeded()) future.complete();
          else {
            // TODO: fallback
            future.complete();
          }
        });
    } else {
      log.warn(cmContext.getNodeId() + " is NOT active.");
      future.complete();
    }
    future.setHandler(resultHandler);
  }

  /**
   * This describers what happens when existing node leaves the cluster.
   */
  @Override
  public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
    Future<Void> resultFuture = Future.future();
    log.trace(cmContext.getNodeId() + " is trying to leave the cluster.");
    if (active) {
      active = false;
      nodeSet.setActive(active);
      // forcibly release all lock being held by node.
      locks.values().forEach(Lock::release);
      nodeSet.remove()
        .compose(aVoid -> destroySession(cmContext.getEphemeralSessionId()))
        .compose(aVoid -> deregisterNode())
        .compose(aVoid -> deregisterTcpCheck())
        .compose(aVoid -> {
          netServer.close();
          cmContext.close();
          return Future.<Void>succeededFuture();
        }).setHandler(resultFuture.completer());
    } else {
      log.warn(cmContext.getNodeId() + "' is NOT active.");
      resultFuture.complete();
    }
    resultFuture.setHandler(resultHandler);
  }

  @Override
  public boolean isActive() {
    return active;
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
    netServer = cmContext.getVertx().createNetServer(new NetServerOptions(nodeTcpAddress));
    netServer.connectHandler(event -> {
    }); // node's tcp server acknowledges consul's heartbeat message.
    netServer.listen(listenEvent -> {
      if (listenEvent.succeeded()) {
        nodeTcpAddress.put("port", listenEvent.result().actualPort());
        future.complete();
      } else future.fail(listenEvent.cause());
    });
    return future;
  }

  /**
   * Gets the vertx node's dedicated service registered within consul agent.
   * TODO: this might get changed.
   */
  private Future<Void> registerService() {
    Future<Void> future = Future.future();
    ServiceOptions serviceOptions = new ServiceOptions();
    serviceOptions.setName(cmContext.getNodeId());
    serviceOptions.setAddress(nodeTcpAddress.getString("host"));
    serviceOptions.setPort(nodeTcpAddress.getInteger("port"));
    serviceOptions.setTags(Collections.singletonList("vertx-clustering"));
    serviceOptions.setId(cmContext.getNodeId());

    cmContext.getConsulClient().registerService(serviceOptions, asyncResult -> {
      if (asyncResult.failed()) {
        log.error("[" + cmContext.getNodeId() + "]" + " - Failed to register node's service.", asyncResult.cause());
        future.fail(asyncResult.cause());
      } else future.complete();
    });
    return future;
  }

  /**
   * Gets the node's tcp check registered within consul .
   */
  private Future<Void> registerTcpCheck() {
    Future<Void> future = Future.future();
    CheckOptions checkOptions = new CheckOptions()
      .setName(checkId)
      .setNotes("This check is dedicated to service with id: " + cmContext.getNodeId())
      .setId(checkId)
      .setTcp(nodeTcpAddress.getString("host") + ":" + nodeTcpAddress.getInteger("port"))
      .setServiceId(cmContext.getNodeId())
      .setInterval(TCP_CHECK_INTERVAL)
      .setDeregisterAfter("10s") // it is still going to be 1 minute.
      .setStatus(CheckStatus.PASSING);
    cmContext.getConsulClient().registerCheck(checkOptions, result -> {
      if (result.failed()) {
        log.error("[" + cmContext.getNodeId() + "]" + " - Failed to register check: " + checkOptions.getId(), result.cause());
        future.fail(result.cause());
      } else future.complete();
    });
    return future;
  }

  /**
   * Gets the vertx node de-registered from consul cluster.
   *
   * @return completed future if vertx node has been successfully de-registered from consul cluster, failed future - otherwise.
   */
  private Future<Void> deregisterNode() {
    Future<Void> future = Future.future();
    cmContext.getConsulClient().deregisterService(cmContext.getNodeId(), event -> {
      if (event.failed()) {
        log.error("[" + cmContext.getNodeId() + "]" + " - Failed to unregister node.", event.cause());
        future.fail(event.cause());
      } else future.complete();
    });
    return future;
  }

  /**
   * Gets the node's tcp check de-registered in consul.
   */
  private Future<Void> deregisterTcpCheck() {
    Future<Void> future = Future.future();
    cmContext.getConsulClient().deregisterCheck(checkId, resultHandler -> {
      if (resultHandler.succeeded()) future.complete();
      else {
        log.error("[" + cmContext.getNodeId() + "]" + " - Failed to deregister check: " + checkId, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Creates consul session. Consul session is used (in context of vertx cluster manager) to create ephemeral map entries.
   *
   * @param sessionName - session name.
   * @return session id.
   */
  private Future<String> registerSession(String sessionName) {
    Future<String> future = Future.future();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(SessionBehavior.DELETE)
      .setLockDelay(0)
      .setName(sessionName)
      .setChecks(Arrays.asList(checkId, "serfHealth"));

    cmContext.getConsulClient().createSessionWithOptions(sessionOptions, session -> {
      if (session.succeeded()) {
        log.trace("[" + cmContext.getNodeId() + "]" + " - " + sessionName + ": " + session.result() + " has been registered for .");
        future.complete(session.result());
      } else {
        log.error("[" + cmContext.getNodeId() + "]" + " - Failed to register the session.", session.cause());
        future.fail(session.cause());
      }
    });
    return future;
  }

  /**
   * Destroys node's session in consul.
   */
  Future<Void> destroySession(String sessionId) {
    Future<Void> future = Future.future();
    cmContext.getConsulClient().destroySession(sessionId, resultHandler -> {
      if (resultHandler.succeeded()) {
        log.trace("[" + cmContext.getNodeId() + "]" + " - Session: " + sessionId + " has been successfully destroyed.");
        future.complete();
      } else {
        log.error("[" + cmContext.getNodeId() + "]" + " - Failed to destroy session: " + sessionId, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  // TODO : DEFINE ConsulSessionManager to remove duplicate code
  private class ConsulSessionManager {
    public void close() {

    }
  }
}
