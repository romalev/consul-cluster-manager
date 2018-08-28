package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static io.vertx.spi.cluster.consul.impl.ClusterSerializationUtils.*;

/**
 * Distributed consul async multimap implementation.
 * <p>
 * <b>Implementation details:</b>
 * <p>
 * - Consul itself doesn't provide out-of-the box the multimap implementation - this is (to be) addressed locally.
 * Entries of vertx event-bus subscribers MUST BE EPHEMERAL (AsyncMultiMap holds the subscribers) so node id is appended to each key of this map.
 * Example :
 * __vertx.subs/users.create.channel-{nodeId} -> localhost:5501
 * __vertx.subs/users.create.channel-{nodeId} -> localhost:5502
 * __vertx.subs/users.push.sms.channel-{nodeId} -> localhost:5505
 * __vertx.subs/users.push.sms.channel-{nodeId} -> localhost:5506
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMultiMap<K, V> extends ConsulMap<K, V> implements AsyncMultiMap<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

    private final Vertx vertx;
    private final String nodeId;
    private final KeyValueOptions kvOpts;

    // FIXME - to achieve round-robin loadbalancing - this cache has to implemnted.
    private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();


    public ConsulAsyncMultiMap(String name, Vertx vertx, ConsulClient consulClient, String sessionId, String nodeId) {
        super(name, consulClient);
        this.vertx = vertx;
        this.nodeId = nodeId;
        // options to make entries of this map ephemeral.
        this.kvOpts = new KeyValueOptions().setAcquireSession(sessionId);
        printOutAsyncMultiMap();
    }

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> encodeF(v))
                .compose(value -> putConsulValue(getConsulMultiMapKey(k.toString()), value, kvOpts))
                .compose(aBoolean -> aBoolean ? Future.succeededFuture() : Future.<Void>failedFuture(k.toString() + " wasn't added to consul kv store."))
                .setHandler(completionHandler);
    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
        assertKeyIsNotNull(k)
                .compose(aVoid -> consulEntries())
                .compose(keyValueList -> {
                    Future<ChoosableIterable<V>> future = Future.future();
                    Set<String> collectedPlainValueSet = getListResult(keyValueList).stream()
                            .filter(keyValue -> keyValue.getKey().contains(k.toString()))
                            .map(KeyValue::getValue)
                            .collect(Collectors.toSet());
                    ChoosableSet<V> choosableSet = new ChoosableSet<>(collectedPlainValueSet.size());
                    collectedPlainValueSet.forEach(s -> {
                        try {
                            choosableSet.add(decode(s));
                        } catch (Exception e) {
                            future.fail(e.getCause());
                        }
                    });
                    future.complete(choosableSet);
                    return future;
                })
                .setHandler(asyncResultHandler);
    }


    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
        assertKeyIsNotNull(k)
                .compose(aVoid -> getConsulKeyValue(getConsulMultiMapKey(k.toString())))
                .compose(keyValue -> decodeF(keyValue.getValue()))
                .compose(value -> {
                    Future<Boolean> future = Future.future();
                    if (value == null) {
                        future.complete(false);
                    } else {
                        removeConsulValue(getConsulMultiMapKey(k.toString())).setHandler(future.completer());
                    }
                    return future;
                })
                .setHandler(completionHandler);
    }

    @Override
    public void removeAllForValue(V v, Handler<AsyncResult<Void>> completionHandler) {
        // TODO : implement this.
    }

    @Override
    public void removeAllMatching(Predicate<V> p, Handler<AsyncResult<Void>> completionHandler) {
        // TODO: implement this.
    }


    // helper method used to print out periodically the async consul map.
    private void printOutAsyncMultiMap() {
        vertx.setPeriodic(15000, event -> consulClient.getValues(name, futureValues -> {
            if (futureValues.succeeded()) {
                if (Objects.nonNull(futureValues.result()) && Objects.nonNull(futureValues.result().getList())) {
                    Map<String, String> asyncMap = futureValues.result().getList().stream().collect(Collectors.toMap(KeyValue::getKey, KeyValue::getValue));
                    log.trace("Consul Async Multimap: '{}' ->  {}", name, Json.encodePrettily(asyncMap));
                } else {
                    log.trace("Consul Async Multimap: '{}' seems to be empty.", name);
                }
            } else {
                log.error("Failed to print out Consul Async Multimap: '{}' due to: '{}' ", name, futureValues.cause().toString());
            }
        }));
    }

    private String getConsulMultiMapKey(String actualKey) {
        return name + "/" + actualKey + "-" + nodeId;
    }
}