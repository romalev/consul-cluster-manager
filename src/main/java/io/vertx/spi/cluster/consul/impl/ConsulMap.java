package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.consul.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.vertx.core.Future.succeededFuture;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureConsulEntry;
import static io.vertx.spi.cluster.consul.impl.ConversationUtils.asFutureString;

/**
 * Abstract map functionality for clustering maps.
 *
 * @author Roman Levytskyi
 */
abstract class ConsulMap<K, V> extends ConsulMapListener {

  private static final Logger log = LoggerFactory.getLogger(ConsulMap.class);

  ConsulMap(String name, CmContext cmContext) {
    super(name, cmContext);
  }

  /**
   * Puts an entry to Consul KV store.
   *
   * @param k - holds the key of an entry.
   * @param v - holds the value of an entry.
   * @return succeededFuture indicating that an entry has been put, failedFuture - otherwise.
   */
  Future<Boolean> putValue(K k, V v) {
    return putValue(k, v, null);
  }

  /**
   * Puts an entry to Consul KV store by taking into account additional options : these options are mainly used to make an entry ephemeral or
   * to place TTL on an entry.
   *
   * @param k               - holds the key of an entry.
   * @param v               - holds the value of an entry.
   * @param keyValueOptions - holds kv options (note: null is allowed)
   * @return succeededFuture indicating that an entry has been put, failedFuture - otherwise.
   */
  Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions) {
    return assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> asFutureString(k, v, context.getNodeId()))
      .compose(value -> putConsulValue(keyPath(k), value, keyValueOptions));
  }

  /**
   * Puts an entry to Consul KV store. Does the actual job.
   *
   * @param key             - holds the consul key of an entry.
   * @param value           - holds the consul value (should be marshaled) of an entry.
   * @param keyValueOptions - holds kv options (note: null is allowed)
   * @return booleanFuture indication the put result (true - an entry has been put, false - otherwise), failedFuture - otherwise.
   */
  Future<Boolean> putConsulValue(String key, String value, KeyValueOptions keyValueOptions) {
    Future<Boolean> future = Future.future();
    context.getConsulClient().putValueWithOptions(key, value, keyValueOptions, resultHandler -> {
      if (resultHandler.succeeded()) {
        // log.trace("[" + nodeId + "] " + key + " -> " + value + " put is " + resultHandler.result());
        future.complete(resultHandler.result());
      } else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to put " + key + " -> " + value, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Gets the value by key.
   *
   * @param k - holds the key.
   * @return either empty future if key doesn't exist in KV store, future containing the value if key exists, failedFuture - otherwise.
   */
  Future<V> getValue(K k) {
    return assertKeyIsNotNull(k)
      .compose(aVoid -> getConsulKeyValue(keyPath(k)))
      .compose(consulValue -> asFutureConsulEntry(consulValue.getValue()))
      .compose(consulEntry -> consulEntry == null ? succeededFuture() : succeededFuture((V) consulEntry.getValue()));
  }

  Future<Map<K, V>> entries() {
    return consulEntries()
      .compose(kvEntries -> {
        List<Future> futureList = new ArrayList<>();
        kvEntries.forEach(kv -> futureList.add(asFutureConsulEntry(kv.getValue())));
        return CompositeFuture.all(futureList).map(compositeFuture -> {
          Map<K, V> map = new HashMap<>();
          for (int i = 0; i < compositeFuture.size(); i++) {
            ConsulEntry<K, V> consulEntry = compositeFuture.resultAt(i);
            map.put(consulEntry.getKey(), consulEntry.getValue());
          }
          return map;
        });
      });
  }

  Future<Boolean> delete(K key) {
    return deleteConsulValue(keyPath(key));
  }

  /**
   * Gets the value by consul key.
   *
   * @param consulKey - holds the consul key.
   * @return either empty future if key doesn't exist in KV store, future containing the value if key exists, failedFuture - otherwise.
   */
  Future<KeyValue> getConsulKeyValue(String consulKey) {
    Future<KeyValue> future = Future.future();
    context.getConsulClient().getValue(consulKey, resultHandler -> {
      if (resultHandler.succeeded()) {
        // note: resultHandler.result().getValue() is null if nothing was found.
        // log.trace("[" + nodeId + "]" + " - Entry is found : " + resultHandler.result().getValue() + " by key: " + consulKey);
        future.complete(resultHandler.result());
      } else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to look up an entry by: " + consulKey, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Remove the key/value pair that corresponding to the specified key.
   */
  Future<Boolean> deleteConsulValue(String key) {
    Future<Boolean> result = Future.future();
    context.getConsulClient().deleteValue(key, resultHandler -> handleDeleteResult(key, result, resultHandler));
    return result;
  }

  /**
   * Removes all the key/value pair that corresponding to the specified key prefix.
   */
  Future<Boolean> deleteConsulValues(String key) {
    Future<Boolean> result = Future.future();
    context.getConsulClient().deleteValues(key, resultHandler -> handleDeleteResult(key, result, resultHandler));
    return result;
  }

  /**
   * Clears the entire map.
   */
  Future<Void> deleteAll() {
    Future<Void> future = Future.future();
    context.getConsulClient().deleteValues(name, result -> {
      if (result.succeeded()) future.complete();
      else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to clear an entire: " + name);
        future.fail(result.cause());
      }
    });
    return future;
  }

  Future<List<String>> consulKeys() {
    Future<List<String>> futureKeys = Future.future();
    context.getConsulClient().getKeys(name, resultHandler -> {
      if (resultHandler.succeeded()) {
        // log.trace("[" + nodeId + "]" + " - Found following keys of: " + name + " -> " + resultHandler.result());
        futureKeys.complete(resultHandler.result());
      } else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to fetch keys of: " + name, resultHandler.cause());
        futureKeys.fail(resultHandler.cause());
      }
    });
    return futureKeys;
  }

  Future<List<KeyValue>> consulEntries() {
    Future<List<KeyValue>> keyValueListFuture = Future.future();
    context.getConsulClient().getValues(name, resultHandler -> {
      if (resultHandler.succeeded()) keyValueListFuture.complete(nullSafeListResult(resultHandler.result()));
      else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to fetch entries of: " + name, resultHandler.cause());
        keyValueListFuture.fail(resultHandler.cause());
      }
    });
    return keyValueListFuture;
  }


  @Override
  protected void entryUpdated(EntryEvent event) {
    // default map listener implementation doesn't start a watch to listen for updates.
  }

  /**
   * Creates consul session. Consul session is used (in context of vertx cluster manager) to create ephemeral map entries.
   *
   * @param sessionName - session name.
   * @param checkId     - id of the check session will get bound to.
   * @return session id.
   */
  Future<String> registerSession(String sessionName, String checkId) {
    Future<String> future = Future.future();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(SessionBehavior.DELETE)
      .setLockDelay(0)
      .setName(sessionName)
      .setChecks(Arrays.asList(checkId, "serfHealth"));

    context.getConsulClient().createSessionWithOptions(sessionOptions, session -> {
      if (session.succeeded()) {
        log.trace("[" + context.getNodeId() + "]" + " - " + sessionName + ": " + session.result() + " has been registered.");
        future.complete(session.result());
      } else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to register the session.", session.cause());
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
    context.getConsulClient().destroySession(sessionId, resultHandler -> {
      if (resultHandler.succeeded()) {
        log.trace("[" + context.getNodeId() + "]" + " - Session: " + sessionId + " has been successfully destroyed.");
        future.complete();
      } else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to destroy session: " + sessionId, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Creates TTL dedicated consul session. TTL on entries is handled by relaying on consul session itself.
   * We have to register the session first in consul and then bound the session's id with entries we want to put ttl on.
   * <p>
   * Note: session invalidation-time is twice the TTL time -> https://github.com/hashicorp/consul/issues/1172
   * (This is done on purpose. The contract of the TTL is that it will not expire before that value, but could expire after.
   * There are number of reasons for that (complexity during leadership transition), but consul devs add a grace period to account for clock skew and network delays.
   * This is to shield the application from dealing with that.)
   *
   * @param ttl - holds ttl in ms, this value must be between {@code 10s} and {@code 86400s} currently.
   * @return session id.
   */
  Future<String> getTtlSessionId(long ttl, K k) {
    if (ttl < 10000) {
      log.warn("[" + context.getNodeId() + "]" + " - Specified ttl is less than allowed in consul -> min ttl is 10s.");
      ttl = 10000;
    }

    if (ttl > 86400000) {
      log.warn("[" + context.getNodeId() + "]" + " - Specified ttl is more that allowed in consul -> max ttl is 86400s.");
      ttl = 86400000;
    }

    String consulKey = keyPath(k);
    String sessionName = "ttlSession_" + consulKey;
    Future<String> future = Future.future();
    SessionOptions sessionOpts = new SessionOptions()
      .setTtl(TimeUnit.MILLISECONDS.toSeconds(ttl))
      .setBehavior(SessionBehavior.DELETE)
      // Lock delay is a time duration, between 0 and 60 seconds. When a session invalidation takes place,
      // Consul prevents any of the previously held locks from being re-acquired for the lock-delay interval
      .setLockDelay(0)
      .setName(sessionName);

    context.getConsulClient().createSessionWithOptions(sessionOpts, idHandler -> {
      if (idHandler.succeeded()) {
        log.trace("[" + context.getNodeId() + "]" + " - TTL session has been created with id: " + idHandler.result());
        future.complete(idHandler.result());
      } else {
        log.error("[" + context.getNodeId() + "]" + " - Failed to create ttl consul session", idHandler.cause());
        future.fail(idHandler.cause());
      }
    });
    return future;
  }


  /**
   * Obtains a result from {@link Future} by and waiting for it's completion.
   * Note: should never be called from event loop context!
   *
   * @param future  - future holding result of future computation.
   * @param timeout - the maximum time to wait in ms.
   * @param <T>     - result type.
   * @return computation result.
   */
  <T> T toSync(Future<T> future, long timeout) {
    CompletableFuture<T> completableFuture = new CompletableFuture<>();
    future.setHandler(event -> {
      if (event.succeeded()) completableFuture.complete(event.result());
      else completableFuture.completeExceptionally(event.cause());
    });
    T result;
    try {
      result = completableFuture.get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw new VertxException(e);
    }
    return result;
  }

  /**
   * Verifies whether value is not null.
   */
  Future<Void> assertValueIsNotNull(Object value) {
    boolean result = value == null;
    if (result) return io.vertx.core.Future.failedFuture("Value can not be null.");
    else return succeededFuture();
  }

  /**
   * Verifies whether key & value are not null.
   */
  Future<Void> assertKeyAndValueAreNotNull(Object key, Object value) {
    return assertKeyIsNotNull(key).compose(aVoid -> assertValueIsNotNull(value));
  }

  /**
   * Verifies whether key is not null.
   */
  Future<Void> assertKeyIsNotNull(Object key) {
    boolean result = key == null;
    if (result) return io.vertx.core.Future.failedFuture("Key can not be null.");
    else return succeededFuture();
  }

  String keyPath(Object k) {
    // we can't simply ship sequence of bytes to consul.
    if (k instanceof Buffer) {
      return name + "/" + Base64.getEncoder().encodeToString(((Buffer) k).getBytes());
    }
    return name + "/" + k.toString();
  }

  String actualKey(String key) {
    return key.replace(name + "/", "");
  }

  /**
   * Returns NULL - safe key value list - simple wrapper around getting list out of {@link KeyValueList} instance.
   */
  List<KeyValue> nullSafeListResult(KeyValueList keyValueList) {
    return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
  }

  /**
   * Handles result of remove operation.
   */
  private void handleDeleteResult(String key, Future<Boolean> result, AsyncResult<Void> resultHandler) {
    if (resultHandler.succeeded()) {
      log.trace("[" + context.getNodeId() + "] " + key + " -> " + " remove is true.");
      result.complete(true);
    } else {
      log.error("[" + context.getNodeId() + "]" + " - Failed to remove an entry by key: " + key, result.cause());
      result.fail(resultHandler.cause());
    }
  }

}
