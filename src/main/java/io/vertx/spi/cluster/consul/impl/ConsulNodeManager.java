package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.spi.cluster.NodeListener;
import io.vertx.ext.consul.*;
import io.vertx.spi.cluster.consul.impl.cache.CacheManager;
import io.vertx.spi.cluster.consul.impl.cache.KvStoreListener;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static io.vertx.spi.cluster.consul.impl.ConversationUtils.decode;

/**
 * Main manager is accountable for:
 * <p>
 * - Node registration within the cluster. Every new consul service and new entry within __vertx.nodes represent actual vertx node.
 * Don't confuse vertx node with consul (native) node - these are completely different things.
 * Note: every vetx nodes that joins the cluster IS tagged with NODE_COMMON_TAG = "vertx-clustering".
 * <p>
 * - Node discovery. Nodes discovery for a new node happening while this new node is joining the cluster - it looks up all entry withing __vertx.nodes.
 * <p>
 * - HaInfo pre-initialization. We need to pre-build haInfo map at the "node is joining" stage to be able later on to properly
 * initialize the consul sync map. We can't block the event loop thread that gets the consul sync map. (Consul sync map holds haInfo.)
 * <p>
 * - Creating dummy TCP server to receive and acknowledge heart beats messages from consul.
 * <p>
 * - Creating TCP check on the consul side that sends heart beats messages to previously mentioned tcp server - this allow consul agent to be aware of
 * what is going on within the cluster (node is active if it acknowledges hear beat message, inactive - otherwise.)
 * <p>
 * - Creating consul session. Consul session is used to make consul map entries ephemeral (every entry with that is created with special KV options referencing session id gets automatically deleted
 * from the consul cluster once session gets invalidated).
 * In this cluster manager case session will get invalidated when:
 * <li>health check gets unregistered.</li>
 * <li>health check goes to critical state (when this.netserver doesn't acknowledge the consul's heartbeat message).</li>
 * <li>session is explicitly destroyed.</li>
 *
 * @author Roman Levytskyi
 */
public class ConsulNodeManager extends ConsulMap<String, String> implements KvStoreListener {

    private static final Logger log = LoggerFactory.getLogger(ConsulNodeManager.class);
    private static final String HA_INFO_MAP = "__vertx.haInfo";
    private static final String TCP_CHECK_INTERVAL = "10s";

    private final Vertx vertx;
    private final CacheManager cM;
    private final String nodeId;
    private final String checkId;
    private final String sessionName;
    private final Map haInfoMap = new ConcurrentHashMap<>(); // dedicated cache to initialize and keep haInfo.
    private JsonObject tcpAddress = new JsonObject(); // tcp address of node.
    // local cache of all vertx cluster nodes.
    private Set<String> nodes = new HashSet<>();
    private NetServer netServer;
    // consul session id used to lock map entries.
    private String sessionId;
    private NodeListener nodeListener;

    public ConsulNodeManager(Vertx vertx, ConsulClient cC, CacheManager cM, String nodeId) {
        super("__vertx.nodes", cC);
        this.vertx = vertx;
        this.nodeId = nodeId;
        this.cM = cM;
        this.checkId = "tcpCheckFor-" + nodeId;
        this.sessionName = "sessionFor-" + nodeId;
        printLocalNodeMap();
    }


    /**
     * Joins node with the cluster in async manner.
     */
    public void join(Handler<AsyncResult<Void>> resultHandler) {
        createTcpServer()
                .compose(aVoid -> registerService())
                .compose(aVoid -> registerTcpCheck())
                .compose(aVoid -> registerSession())
                .compose(aVoid -> registerNode())
                .compose(aVoid -> discoverNodes())
                .compose(aVoid -> initHaInfo())
                .setHandler(resultHandler);
    }

    /**
     * Node leaving the cluster.
     */
    public void leave(Handler<AsyncResult<Void>> resultHandler) {
        Future.succeededFuture()
                .compose(aVoid -> {
                    haInfoMap.clear();
                    netServer.close();
                    return Future.<Void>succeededFuture();
                })
                .compose(aVoid -> destroySession())
                .compose(aVoid -> deregisterNode())
                .compose(aVoid -> deregisterTcpCheck())
                .setHandler(resultHandler);
    }

    /**
     * @return available nodes in cluster.
     */
    public List<String> getNodes() {
        return new ArrayList<>(nodes);
    }

    /**
     * @return session id - used to make consul map entries ephemeral.
     */
    public String getSessionId() {
        return sessionId;
    }

    @Override
    public void entryUpdated(EntryEvent event) {
        vertx.executeBlocking(workingThread -> {
            String receivedNodeId = actualKey(event.getEntry().getKey());
            switch (event.getEventType()) {
                case WRITE: {
                    log.trace("Node: '{}' has joined the cluster.", receivedNodeId);
                    nodes.add(receivedNodeId);
                    if (nodeListener != null) {
                        log.trace("Adding new node: '{}' to nodeListener.", receivedNodeId);
                        nodeListener.nodeAdded(receivedNodeId);
                    }
                    break;
                }
                case REMOVE: {
                    nodes.remove(receivedNodeId);
                    log.trace("Node: '{}' has left the cluster.", receivedNodeId);
                    if (nodeListener != null) {
                        log.trace("Removing an existing node: '{}' from nodeListener.", receivedNodeId);
                        nodeListener.nodeLeft(receivedNodeId);
                    }
                    break;
                }
            }
            workingThread.complete();
        }, result -> {
        });

    }

    public void initNodeListener(NodeListener nodeListener) {
        this.nodeListener = nodeListener;
        Watch<KeyValueList> watch = cM.createAndGetMapWatch(name);
        watch.setHandler(kvWatchHandler()).start();
    }


    /**
     * @param <K> represents key type.
     * @param <V> represents value type.
     * @return pre-initialized cache that is later used to build consul sync map.
     */
    public <K, V> Map<K, V> getHaInfo() {
        return haInfoMap;
    }

    /**
     * Initializes haInfo map.
     *
     * @param <K> represents key type.
     * @param <V> represents value type.
     * @return completed future if haInfo is initialized successfully, failed future - otherwise.
     */
    private <K, V> Future<Void> initHaInfo() {
        log.trace("Initializing: '{}' internal cache ... ", HA_INFO_MAP);
        Future<Void> futureHaInfoCache = Future.future();
        consulClient.getValues(HA_INFO_MAP, futureMap -> {
            if (futureMap.succeeded()) {
                List<KeyValue> keyValueList = getKeyValueListOrEmptyList(futureMap.result());
                keyValueList.forEach(keyValue -> {
                    try {
                        ConversationUtils.GenericEntry<K, V> entry = decode(keyValue.getValue());
                        haInfoMap.put(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        log.error("Can't decode value: {} while pre-init haInfo cache.", e.getCause().toString());
                        futureHaInfoCache.fail(e);
                    }
                });
                log.trace("'{}' internal cache is pre-built now: '{}'", HA_INFO_MAP, Json.encodePrettily(haInfoMap));
                futureHaInfoCache.complete();
            } else {
                log.error("Can't initialize the : '{}' due to: '{}'", HA_INFO_MAP, futureMap.cause().toString());
                futureHaInfoCache.fail(futureMap.cause());
            }
        });
        return futureHaInfoCache;
    }

    /**
     * Gets the vertx node's dedicated service registered within consul agent.
     */
    private Future<Void> registerService() {
        Future<Void> future = Future.future();
        ServiceOptions serviceOptions = new ServiceOptions();
        serviceOptions.setName(nodeId);
        serviceOptions.setAddress(tcpAddress.getString("host"));
        serviceOptions.setPort(tcpAddress.getInteger("port"));
        serviceOptions.setTags(Collections.singletonList("vertx-clustering"));
        serviceOptions.setId(nodeId);

        consulClient.registerService(serviceOptions, asyncResult -> {
            if (asyncResult.failed()) {
                netServer.close();
                log.error("Can't register node: '{}' due to: '{}'", nodeId, asyncResult.cause());
                future.fail(asyncResult.cause());
            } else future.complete();
        });
        return future;
    }

    private Future<Void> registerNode() {
        Future<Void> future = Future.future();
        putValue(nodeId, tcpAddress.encode(), new KeyValueOptions().setAcquireSession(sessionId)).setHandler(asyncResult -> {
            if (asyncResult.failed()) {
                netServer.close();
                log.error("Can't add node: '{}' to: '{}' due to: '{}'", nodeId, name, asyncResult.cause());
                future.fail(asyncResult.cause());
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
        consulClient.deregisterService(nodeId, event -> {
            if (event.succeeded()) {
                log.trace("'{}' has been unregistered.");
                future.complete();
            } else {
                log.error("Couldn't unregister node: '{}' due to: '{}'", nodeId, event.cause().toString());
                future.fail(event.cause());
            }
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
                .setTcp(tcpAddress.getString("host") + ":" + tcpAddress.getInteger("port"))
                .setServiceId(nodeId)
                .setInterval(TCP_CHECK_INTERVAL)
                .setDeregisterAfter("10s") // it is still going to be 1 minute.
                .setStatus(CheckStatus.PASSING);

        consulClient.registerCheck(checkOptions, result -> {
            if (result.succeeded()) {
                log.trace("Check has been registered : '{}'", checkOptions.getId());
                future.complete();
            } else {
                log.trace("Can't register check: '{}' due to: '{}'", checkOptions.getId(), result.cause().toString());
                // try to de-register the node from consul cluster
                deregisterNode();
                future.fail(result.cause());
            }
        });
        return future;
    }

    /**
     * Gets the node's tcp check de-registered in consul.
     */
    private Future<Void> deregisterTcpCheck() {
        Future<Void> future = Future.future();
        consulClient.deregisterCheck(checkId, resultHandler -> {
            if (resultHandler.succeeded()) future.complete();
            else {
                log.error("Can't deregister check: '{}' for node: '{}' due to: '{}'", checkId, nodeId, resultHandler.cause());
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }

    /**
     * Discovers nodes that are currently available within the cluster.
     *
     * @return completed future if nodes (consul services) have been successfully fetched from consul cluster,
     * failed future - otherwise.
     */
    private Future<Void> discoverNodes() {
        log.trace("Trying to discover nodes that are available within the consul cluster.");
        return consulKeys()
                .compose(list -> {
                    if (list == null) return Future.succeededFuture();
                    nodes = list.stream().map(this::actualKey).collect(Collectors.toSet());
                    log.trace("Available nodes within the cluster: '{}'", nodes);
                    return Future.succeededFuture();
                });
    }

    /**
     * Creates consul session. Consul session is used (in context of vertx cluster manager) to create ephemeral map entries.
     *
     * @return completed future if consul session (consul services) has been successfully registered in consul cluster,
     * failed future - otherwise.
     */
    private Future<Void> registerSession() {
        Future<Void> future = Future.future();
        SessionOptions sessionOptions = new SessionOptions()
                .setBehavior(SessionBehavior.DELETE)
                .setName(sessionName)
                .setChecks(Arrays.asList(checkId, "serfHealth"));

        consulClient.createSessionWithOptions(sessionOptions, session -> {
            if (session.succeeded()) {
                log.trace("Session : '{}' has been registered.", session.result());
                sessionId = session.result();
                future.complete();
            } else {
                log.error("Couldn't register the session due to: {}", session.cause().toString());
                future.fail(session.cause());
            }
        });
        return future;
    }

    /**
     * Destroys node's session in consul.
     */
    private Future<Void> destroySession() {
        Future<Void> future = Future.future();
        consulClient.destroySession(sessionId, resultHandler -> {
            if (resultHandler.succeeded()) {
                log.trace("Session: '{}' has been successfully destroyed for node: '{}'.", sessionId, nodeId);
            } else {
                log.error("Can't destroy session: '{}' for node: '{}' due to: '{}'", sessionId, resultHandler.cause(), nodeId);
                future.fail(resultHandler.cause());
            }
        });
        return future;
    }

    /**
     * Creates simple tcp server used to receive heart beat messages from consul cluster and acknowledge them.
     */
    private Future<Void> createTcpServer() {
        Future<Void> future = Future.future();
        try {
            tcpAddress.put("host", InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            log.error(e);
            future.fail(e);
        }
        netServer = vertx.createNetServer(new NetServerOptions(tcpAddress));
        netServer.connectHandler(event -> {
        }); // node's tcp server acknowledges consul's heartbeat message.
        netServer.listen(listenEvent -> {
            if (listenEvent.succeeded()) {
                tcpAddress.put("port", listenEvent.result().actualPort());
                future.complete();
            } else future.fail(listenEvent.cause());
        });
        return future;
    }

    // TODO: remove it.
    private void printLocalNodeMap() {
        vertx.setPeriodic(15000, handler -> log.trace("Nodes are: '{}'", Json.encodePrettily(nodes)));
    }
}
