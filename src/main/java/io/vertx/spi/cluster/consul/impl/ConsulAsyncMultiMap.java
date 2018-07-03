package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Distributed async multimap implementation based on consul key-value store.
 * <p>
 * TODO: 1) most of logging has to be removed when consul cluster manager is more or less stable.
 * TODO: 2) everything has to be documented in javadocs.
 * TODO: 3) Marshalling and unmarshalling.
 * TODO: 4) Some caching perhaps ???
 *
 * @author Roman Levytskyi
 */
public class ConsulAsyncMultiMap<K, V> extends ConsulAsyncAbstractMap<K, V> implements AsyncMultiMap<K, V> {

    private final static Logger log = LoggerFactory.getLogger(ConsulAsyncMultiMap.class);

    private final ConsulClient consulClient;
    private final Vertx vertx;
    private final String name;

    // TODO: consider adding a cache in cast the connection between node and consul in unstable.
    private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

    public ConsulAsyncMultiMap(String name, Vertx vertx, ConsulClient consulClient) {
        this.name = name;
        this.consulClient = consulClient;
        this.vertx = vertx;
        printOutAsyncMultiMap();
    }

    @Override
    public void add(K k, V v, Handler<AsyncResult<Void>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> {
                    log.trace("Adding KV: '{}'->'{}' to Consul Async Multimap: '{}'", getConsulKey(name, k), v.toString(), name);
                    return getEntry(k);
                })
                .compose(vs -> {
                    String consulKey = getConsulKey(name, k);
                    Future<Void> future = Future.future();
                    if (vs.contains(v)) {
                        log.trace("V: '{}' already exists within: '{}' in: '{}'", v.toString(), vs, name);
                        future.complete();
                    } else {
                        vs.add(v);
                        String value = marshalValue(vs);
                        consulClient.putValue(consulKey, value, resultHandler -> {
                            if (resultHandler.succeeded()) {
                                log.trace("KV: '{}'->'{}' has been added to Consul Async Multimap: '{}'.", consulKey, value, name);
                                future.complete();
                            } else {
                                log.error("Can't add/update an entry KV: '{}'->'{}' in Consul Async Multimap: '{}' due to: '{}' ", consulKey, value, name, resultHandler.cause().toString());
                                future.fail(resultHandler.cause());
                            }
                        });
                    }
                    return future;
                })
                .setHandler(completionHandler);
    }

    @Override
    public void get(K k, Handler<AsyncResult<ChoosableIterable<V>>> asyncResultHandler) {
        assertKeyIsNotNull(k)
                .compose(aVoid -> getEntry(k))
                .compose(aSet -> {
                    Future<ChoosableIterable<V>> future = Future.future();
                    ChoosableSet<V> newEntries = new ChoosableSet<>(aSet.size());
                    aSet.forEach(newEntries::add);
                    future.complete(newEntries);
                    return future;
                })
                .setHandler(asyncResultHandler);
    }


    @Override
    public void remove(K k, V v, Handler<AsyncResult<Boolean>> completionHandler) {
        assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> getEntry(k))
                .compose(aSet -> {
                    Future<Boolean> future = Future.future();
                    String consulKey = getConsulKey(name, k);
                    if (aSet.remove(v)) {
                        log.trace("V: '{}' already exists within: '{}' in: '{}' so removing it.", v.toString(), aSet, name);
                        String encodedSet = marshalValue(aSet);
                        consulClient.putValue(consulKey, encodedSet, resultHandler -> {
                            if (resultHandler.succeeded()) {
                                log.error("V: '{}' has been removed from: '{}' by K: '{}' in: '{}' due to: '{}'", v.toString(), aSet, consulKey, aSet, resultHandler.cause().toString());
                                future.complete(true);
                            } else {
                                log.error("Can't remove V: '{}' from: '{}' by K: '{}' in: '{}' due to: '{}'", v.toString(), aSet, consulKey, aSet, resultHandler.cause().toString());
                                future.fail(resultHandler.cause());
                            }
                        });

                    } else {
                        log.warn("V: '{}' doesn't exists within: '{}' in: '{}' so can't get it removed.", v.toString(), aSet, name);
                        future.complete(false);
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

    /**
     * Gets an future entry (set) that k point to.
     */
    private Future<Set<V>> getEntry(K k) {
        String consulKey = getConsulKey(name, k);
        log.trace("Getting an entry by K: '{}' from Consul Async Multimap: '{}'.", consulKey);
        Future<Set<V>> futureValue = Future.future();
        consulClient.getValue(consulKey, resultHandler -> {
            if (resultHandler.succeeded()) {
                Set<V> set = unMarshalValue(resultHandler.result().getValue());
                log.trace("Got V: '{}' by K: '{}' from  from Consul Async Multimap: '{}'.", set, consulKey, name);
                futureValue.complete(set);
            } else {
                log.error("Can't get an entry by K: '{}' from Consul Async Multimap: '{}' due to: '{}'.", consulKey, name, resultHandler.cause().toString());
                futureValue.fail(resultHandler.cause());
            }
        });
        return futureValue;
    }

    private Set<V> unMarshalValue(String value) {
        Map<String, Object> map = new JsonObject(value).getMap();
        if (!map.isEmpty()) {
            // not really safe.
            return new HashSet(map.values());
        } else {
            return new HashSet<>();
        }
    }

    private String marshalValue(Set<V> value) {
        JsonObject jsonObject = new JsonObject();
        value.forEach(v -> jsonObject.put(UUID.randomUUID().toString(), v.toString()));
        return jsonObject.encodePrettily();

    }

    // helper method used to print out periodically the async consul map.
    private void printOutAsyncMultiMap() {
        vertx.setPeriodic(10000, event -> consulClient.getValues(name, futureValues -> {
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


    // TODO : clean this up!
//    public static void main(String[] args) {
//        ConsulAsyncMultiMap asyncMultiMap = new ConsulAsyncMultiMap(null, null, null);
//        Set<String> set = new HashSet<>();
//        set.add("1");
//        set.add("2");
//
//        set.remove("1");
//
//        String result = asyncMultiMap.marshalValue(set);
//        System.out.println(result);
//
//        Set<String> resultSet = asyncMultiMap.unMarshalValue(result);
//        System.out.println(resultSet);
//    }
}
