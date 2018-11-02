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
 *
 * @author Roman Levytskyi
 */
public class ConsulClusterManager implements ClusterManager {

  private static final Logger log = LoggerFactory.getLogger(ConsulClusterManager.class);
  private static final String HA_INFO = "__vertx.haInfo";
  private static final String TCP_CHECK_INTERVAL = "10s";
  private final String nodeId;
  private final ConsulClientOptions cClOptns;
  private final Map<String, Lock> locks = new ConcurrentHashMap<>();
  private final Map<String, Counter> counters = new ConcurrentHashMap<>();
  private final Map<String, Map<?, ?>> syncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMap<?, ?>> asyncMaps = new ConcurrentHashMap<>();
  private final Map<String, AsyncMultiMap<?, ?>> asyncMultiMaps = new ConcurrentHashMap<>();
  private ConsulClusterNodeSet nodeSet;
  private Vertx vertx;
  private ConsulClient cC;
  private String checkId; // tcp consul check id
  private String ephemeralSessionId; // consul session id used to make map entries ephemeral.
  private JsonObject nodeTcpAddress = new JsonObject();
  private NetServer netServer; // dummy TCP server to receive and acknowledge heart beats messages from consul.

  private volatile boolean active;

  public ConsulClusterManager(final ConsulClientOptions options) {
    Objects.requireNonNull(options, "Consul client options can't be null");
    this.cClOptns = options;
    this.nodeId = UUID.randomUUID().toString();
    this.checkId = nodeId;
  }

  public ConsulClusterManager() {
    this.cClOptns = new ConsulClientOptions();
    this.nodeId = UUID.randomUUID().toString();
    this.checkId = nodeId;
  }

  @Override
  public void setVertx(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public <K, V> void getAsyncMultiMap(String name, Handler<AsyncResult<AsyncMultiMap<K, V>>> asyncResultHandler) {
    Future<AsyncMultiMap<K, V>> futureAsyncMultiMap = Future.future();
    AsyncMultiMap asyncMultiMap = asyncMultiMaps.computeIfAbsent(name, key -> new ConsulAsyncMultiMap<>(name, vertx, cC, cClOptns, ephemeralSessionId, nodeId));
    futureAsyncMultiMap.complete(asyncMultiMap);
    futureAsyncMultiMap.setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> void getAsyncMap(String name, Handler<AsyncResult<AsyncMap<K, V>>> asyncResultHandler) {
    Future<AsyncMap<K, V>> futureAsyncMap = Future.future();
    AsyncMap asyncMap = asyncMaps.computeIfAbsent(name, key -> new ConsulAsyncMap<>(name, nodeId, vertx, cC));
    futureAsyncMap.complete(asyncMap);
    futureAsyncMap.setHandler(asyncResultHandler);
  }

  @Override
  public <K, V> Map<K, V> getSyncMap(String name) {
    return (Map<K, V>) syncMaps.computeIfAbsent(name, key -> new ConsulSyncMap<>(name, nodeId, vertx, cC));
  }

  @Override
  public void getLockWithTimeout(String name, long timeout, Handler<AsyncResult<Lock>> resultHandler) {
    vertx.executeBlocking(futureLock -> {
      ConsulLock lock = new ConsulLock(name, nodeId, checkId, timeout, vertx, cC);
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
    Counter counter = counters.computeIfAbsent(name, key -> new ConsulCounter(name, nodeId, vertx, cC));
    counterFuture.complete(counter);
    counterFuture.setHandler(resultHandler);
  }

  @Override
  public String getNodeID() {
    return nodeId;
  }

  @Override
  public List<String> getNodes() {
    return nodeSet.get();
  }

  @Override
  public void nodeListener(NodeListener listener) {
    nodeSet.nodeListener(listener);
  }

  @Override
  public synchronized void join(Handler<AsyncResult<Void>> resultHandler) {
    Future<Void> future = Future.future();
    log.trace(nodeId + " is trying to join the cluster.");
    if (!active) {
      active = true;
      try {
        cC = ConsulClient.create(vertx, cClOptns);
        nodeSet = new ConsulClusterNodeSet(nodeId, vertx, cC, cClOptns, ephemeralSessionId);
      } catch (final Exception e) {
        future.fail(e);
      }
      doJoin().setHandler(future.completer());
      // TODO: fallback
    } else {
      log.warn(nodeId + " is NOT active.");
      future.complete();
    }
    future.setHandler(resultHandler);
  }

  @Override
  public synchronized void leave(Handler<AsyncResult<Void>> resultHandler) {
    Future<Void> resultFuture = Future.future();
    log.trace(nodeId + " is trying to leave the cluster.");
    if (active) {
      active = false;
      // forcibly release all lock being held by node.
      locks.values().forEach(Lock::release);
      doLeave().setHandler(resultFuture.completer());
    } else {
      log.warn(nodeId + "' is NOT active.");
      resultFuture.complete();
    }
    resultFuture.setHandler(resultHandler);
  }

  @Override
  public boolean isActive() {
    return active;
  }


  /**
   * New node joining the cluster behavior. So it does :
   * <p>
   * - Node registration within the cluster. Every new consul service and new entry within __vertx.nodes represent actual vertx node.
   * Don't confuse vertx node with consul (native) node - these are completely different things.
   * Note: every vetx node that joins the cluster IS tagged with NODE_COMMON_TAG = "vertx-clustering".
   * <p>
   * - Cluster nodes discovery. New node while joining the cluster discovers available nodes - it looks up all entry withing __vertx.nodes.
   * <p>
   * - Creating dummy TCP server to receive and acknowledge heart beats messages from consul.
   * <p>
   * - Creating TCP check on the consul side that sends heart beats messages to previously mentioned tcp server - this allow consul agent to be aware of
   * what is going on within the cluster's nodes (node is active if it acknowledges hear beat message, inactive - otherwise.)
   * <p>
   * - Creating consul session. Consul session is used to make consul map entries ephemeral (every entry with that is created with special KV options referencing session id gets automatically deleted
   * from the consul cluster once session gets invalidated).
   * In this cluster manager case session will get invalidated when:
   * <li>health check gets unregistered.</li>
   * <li>health check goes to critical state (when this.netserver doesn't acknowledge the consul's heartbeat message).</li>
   * <li>session is explicitly destroyed.</li>
   */
  private Future<Void> doJoin() {
    return createTcpServer()
      .compose(aVoid -> registerService())
      .compose(aVoid -> registerTcpCheck())
      .compose(aVoid ->
        registerSession("Session for ephemeral keys for: " + nodeId, SessionBehavior.DELETE)
          .compose(s -> {
            ephemeralSessionId = s;
            return succeededFuture();
          }))
      .compose(aVoid -> {
        nodeSet = new ConsulClusterNodeSet(nodeId, vertx, cC, cClOptns, ephemeralSessionId);
        return succeededFuture();
      })
      .compose(aVoid -> {
        Future<Void> future = Future.future();
        ((ConsulSyncMap) getSyncMap(HA_INFO)).clear(future.completer());
        return future;
      })
      .compose(aVoid -> nodeSet.add(nodeTcpAddress))
      .compose(aVoid -> nodeSet.discover());
  }

  /**
   * Existing node leaving the cluster behaviour.
   */
  private Future<Void> doLeave() {
    netServer.close();
    // stop watches and clear caches.
    nodeSet.close(handler -> {
    });
    asyncMultiMaps.values().forEach(map -> ((Closeable) map).close(handler -> {
    }));
    return destroySession(ephemeralSessionId)
      .compose(aVoid -> deregisterNode())
      .compose(aVoid -> deregisterTcpCheck());
  }


  /**
   * Creates simple tcp server used to receive heart beat messages from consul cluster and acknowledge them.
   */
  private Future<Void> createTcpServer() {
    Future<Void> future = Future.future();
    try {
      nodeTcpAddress.put("host", InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException e) {
      log.error(e);
      future.fail(e);
    }
    netServer = vertx.createNetServer(new NetServerOptions(nodeTcpAddress));
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
   */
  private Future<Void> registerService() {
    Future<Void> future = Future.future();
    ServiceOptions serviceOptions = new ServiceOptions();
    serviceOptions.setName(nodeId);
    serviceOptions.setAddress(nodeTcpAddress.getString("host"));
    serviceOptions.setPort(nodeTcpAddress.getInteger("port"));
    serviceOptions.setTags(Collections.singletonList("vertx-clustering"));
    serviceOptions.setId(nodeId);

    cC.registerService(serviceOptions, asyncResult -> {
      if (asyncResult.failed()) {
        log.error("[" + nodeId + "]" + " - Failed to register node's service.", asyncResult.cause());
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
      .setNotes("This check is dedicated to service with id: " + nodeId)
      .setId(checkId)
      .setTcp(nodeTcpAddress.getString("host") + ":" + nodeTcpAddress.getInteger("port"))
      .setServiceId(nodeId)
      .setInterval(TCP_CHECK_INTERVAL)
      .setDeregisterAfter("10s") // it is still going to be 1 minute.
      .setStatus(CheckStatus.PASSING);
    cC.registerCheck(checkOptions, result -> {
      if (result.failed()) {
        log.error("[" + nodeId + "]" + " - Failed to register check: " + checkOptions.getId(), result.cause());
        future.fail(result.cause());
      } else future.complete();
    });
    return future;
  }

  /**
   * Creates consul session. Consul session is used (in context of vertx cluster manager) to create ephemeral map entries.
   *
   * @param sessionName - session name.
   * @return session id.
   */
  private Future<String> registerSession(String sessionName, SessionBehavior sessionBehavior) {
    Future<String> future = Future.future();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(sessionBehavior)
      .setLockDelay(0)
      .setName(sessionName)
      .setChecks(Arrays.asList(checkId, "serfHealth"));

    cC.createSessionWithOptions(sessionOptions, session -> {
      if (session.succeeded()) {
        log.trace("[" + nodeId + "]" + " - " + sessionName + ": " + session.result() + " has been registered for .");
        future.complete(session.result());
      } else {
        log.error("[" + nodeId + "]" + " - Failed to register the session.", session.cause());
        future.fail(session.cause());
      }
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
    cC.deregisterService(nodeId, event -> {
      if (event.failed()) {
        log.error("[" + nodeId + "]" + " - Failed to unregister node.", event.cause());
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
    cC.deregisterCheck(checkId, resultHandler -> {
      if (resultHandler.succeeded()) future.complete();
      else {
        log.error("[" + nodeId + "]" + " - Failed to deregister check: " + checkId, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Destroys node's session in consul.
   */
  Future<Void> destroySession(String sessionId) {
    Future<Void> future = Future.future();
    cC.destroySession(sessionId, resultHandler -> {
      if (resultHandler.succeeded()) {
        log.trace("[" + nodeId + "]" + " - Session: " + sessionId + " has been successfully destroyed.");
        future.complete();
      } else {
        log.error("[" + nodeId + "]" + " - Failed to destroy session: " + sessionId, resultHandler.cause());
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
