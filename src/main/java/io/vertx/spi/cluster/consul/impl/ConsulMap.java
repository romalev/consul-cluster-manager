package io.vertx.spi.cluster.consul.impl;

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
 * @author <a href="mailto:roman.levytskyi.oss@gmail.com">Roman Levytskyi</a>
 */
public abstract class ConsulMap<K, V> extends ConsulMapListener {

  private static final Logger log = LoggerFactory.getLogger(ConsulMap.class);

  protected ConsulMap(String name, ConsulMapContext mapContext) {
    super(name, mapContext);
  }

  /**
   * Puts an entry to Consul KV store.
   *
   * @param k - holds the key of an entry.
   * @param v - holds the value of an entry.
   * @return {@link Future}} containing result.
   */
  Future<Boolean> putValue(K k, V v) {
    return putValue(k, v, null);
  }

  /**
   * Puts an entry to Consul KV store by taking into account additional options
   * (these options are mainly used to make an entry ephemeral or to place TTL on an entry).
   *
   * @param k               - holds the key of an entry.
   * @param v               - holds the value of an entry.
   * @param keyValueOptions - holds kv options (note: null is allowed)
   * @return {@link Future}} containing result.
   */
  Future<Boolean> putValue(K k, V v, KeyValueOptions keyValueOptions) {
    return assertKeyAndValueAreNotNull(k, v)
      .compose(aVoid -> asFutureString(k, v, mapContext.getNodeId()))
      .compose(value -> putPlainValue(keyPath(k), value, keyValueOptions));
  }

  /**
   * Puts plain entry {@link String key} and {@link String value} to Consul KV store.
   *
   * @param key             - holds the consul key of an entry.
   * @param value           - holds the consul value (should be marshaled) of an entry.
   * @param keyValueOptions - holds kv options (note: null is allowed)
   * @return {@link Future}} containing result.
   */
  protected Future<Boolean> putPlainValue(String key, String value, KeyValueOptions keyValueOptions) {
    Future<Boolean> future = Future.future();
    mapContext.getConsulClient().putValueWithOptions(key, value, keyValueOptions, resultHandler -> {
      if (resultHandler.succeeded()) {
        String traceMessage = "[" + mapContext.getNodeId() + "] " + key + " put is " + resultHandler.result();
        if (keyValueOptions != null) {
          log.trace(traceMessage + " with : " + keyValueOptions.getAcquireSession());
        } else {
          log.trace(traceMessage);
        }
        future.complete(resultHandler.result());
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to put " + key + " -> " + value, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Gets the value by key.
   *
   * @param k - holds the key.
   * @return @return {@link Future}} containing result.
   */
  Future<V> getValue(K k) {
    return assertKeyIsNotNull(k)
      .compose(aVoid -> getPlainValue(keyPath(k)))
      .compose(consulValue -> asFutureConsulEntry(consulValue.getValue()))
      .compose(consulEntry -> consulEntry == null ? succeededFuture() : succeededFuture((V) consulEntry.getValue()));
  }

  /**
   * Gets the plain {@link String} value by plain {@link String} key.
   *
   * @param consulKey - holds the consul key.
   * @return @return {@link Future}} containing result.
   */
  Future<KeyValue> getPlainValue(String consulKey) {
    Future<KeyValue> future = Future.future();
    mapContext.getConsulClient().getValue(consulKey, resultHandler -> {
      if (resultHandler.succeeded()) {
        // note: resultHandler.result().getValue() is null if nothing was found.
        // log.trace("[" + nodeId + "]" + " - Entry is found : " + resultHandler.result().getValue() + " by key: " + consulKey);
        future.complete(resultHandler.result());
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to look up an entry by: " + consulKey, resultHandler.cause());
        future.fail(resultHandler.cause());
      }
    });
    return future;
  }

  /**
   * Gets all map's entries.
   *
   * @return @return {@link Future}} containing result.
   */
  Future<Map<K, V>> entries() {
    return plainEntries()
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

  /**
   * Removes an entry by key.
   *
   * @param key holds the key.
   * @return @return {@link Future}} containing result.
   */
  Future<Boolean> deleteValue(K key) {
    return deleteValueByKeyPath(keyPath(key));
  }

  /**
   * Removes an entry by keyPath.
   *
   * @param keyPath - holds the plain {@link String} keyPath.
   * @return @return {@link Future}} containing result.
   */
  protected Future<Boolean> deleteValueByKeyPath(String keyPath) {
    Future<Boolean> result = Future.future();
    mapContext.getConsulClient().deleteValue(keyPath, resultHandler -> {
      if (resultHandler.succeeded()) {
        log.trace("[" + mapContext.getNodeId() + "] " + keyPath + " -> " + " remove is true.");
        result.complete(true);
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to remove an entry by keyPath: " + keyPath, result.cause());
        result.fail(resultHandler.cause());
      }
    });
    return result;
  }

  /**
   * Deletes the entire map.
   */
  Future<Void> deleteAll() {
    Future<Void> future = Future.future();
    mapContext.getConsulClient().deleteValues(name, result -> {
      if (result.succeeded()) future.complete();
      else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to clear an entire: " + name);
        future.fail(result.cause());
      }
    });
    return future;
  }

  /**
   * @return {@link Future} of plain consul kv map's keys.
   */
  protected Future<List<String>> plainKeys() {
    Future<List<String>> futureKeys = Future.future();
    mapContext.getConsulClient().getKeys(name, resultHandler -> {
      if (resultHandler.succeeded()) {
        // log.trace("[" + nodeId + "]" + " - Found following keys of: " + name + " -> " + resultHandler.result());
        futureKeys.complete(resultHandler.result());
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to fetch keys of: " + name, resultHandler.cause());
        futureKeys.fail(resultHandler.cause());
      }
    });
    return futureKeys;
  }

  /**
   * @return {@link Future} of plain consul kv map's entries.
   */
  Future<List<KeyValue>> plainEntries() {
    Future<List<KeyValue>> keyValueListFuture = Future.future();
    mapContext.getConsulClient().getValues(name, resultHandler -> {
      if (resultHandler.succeeded()) keyValueListFuture.complete(nullSafeListResult(resultHandler.result()));
      else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to fetch entries of: " + name, resultHandler.cause());
        keyValueListFuture.fail(resultHandler.cause());
      }
    });
    return keyValueListFuture;
  }

  /**
   * Creates consul session.
   *
   * @param sessionName - session name.
   * @param checkId     - id of the tcp check session will get bound to.
   * @return {@link Future} session id.
   */
  protected Future<String> registerSession(String sessionName, String checkId) {
    Future<String> future = Future.future();
    SessionOptions sessionOptions = new SessionOptions()
      .setBehavior(SessionBehavior.DELETE)
      .setLockDelay(0)
      .setName(sessionName)
      .setChecks(Arrays.asList(checkId, "serfHealth"));

    mapContext.getConsulClient().createSessionWithOptions(sessionOptions, session -> {
      if (session.succeeded()) {
        log.trace("[" + mapContext.getNodeId() + "]" + " - " + sessionName + ": " + session.result() + " has been registered.");
        future.complete(session.result());
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to register the session.", session.cause());
        future.fail(session.cause());
      }
    });
    return future;
  }

  /**
   * Destroys node's session in consul.
   */
  protected Future<Void> destroySession(String sessionId) {
    Future<Void> future = Future.future();
    mapContext.getConsulClient().destroySession(sessionId, resultHandler -> {
      if (resultHandler.succeeded()) {
        log.trace("[" + mapContext.getNodeId() + "]" + " - Session: " + sessionId + " has been successfully destroyed.");
        future.complete();
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to destroy session: " + sessionId, resultHandler.cause());
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
   * <p>
   * Note: method gets deprecated since vert.x cluster management SPI can't be satisfied with using plain consul sessions :(
   */
  @Deprecated
  protected Future<String> getTtlSessionId(long ttl, K k) {
    if (ttl < 10000) {
      log.warn("[" + mapContext.getNodeId() + "]" + " - Specified ttl is less than allowed in consul -> min ttl is 10s.");
      ttl = 10000;
    }

    if (ttl > 86400000) {
      log.warn("[" + mapContext.getNodeId() + "]" + " - Specified ttl is more that allowed in consul -> max ttl is 86400s.");
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

    mapContext.getConsulClient().createSessionWithOptions(sessionOpts, idHandler -> {
      if (idHandler.succeeded()) {
        log.trace("[" + mapContext.getNodeId() + "]" + " - TTL session has been created with id: " + idHandler.result());
        future.complete(idHandler.result());
      } else {
        log.error("[" + mapContext.getNodeId() + "]" + " - Failed to create ttl consul session", idHandler.cause());
        future.fail(idHandler.cause());
      }
    });
    return future;
  }


  /**
   * Obtains a result from {@link Future} by and waiting for it's completion.
   * Note: should never be called on vert.x event loop context!
   *
   * @param future  - holds the work that needs to be executed.
   * @param timeout - the maximum time to wait in ms for work to complete.
   * @param <T>     - work's result type.
   * @return actual computation result.
   */
  protected <T> T completeAndGet(Future<T> future, long timeout) {
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

  @Override
  protected void entryUpdated(EntryEvent event) {
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

  /**
   * Builds a key path to consul map.
   * Later on this key path should be used to access any entry of given consul map.
   *
   * @param k actual key.
   * @return key path.
   */
  protected String keyPath(Object k) {
    // we can't simply ship sequence of bytes to consul.
    if (k instanceof Buffer) {
      return name + "/" + Base64.getEncoder().encodeToString(((Buffer) k).getBytes());
    }
    return name + "/" + k.toString();
  }

  /**
   * Extracts an actual keyPath of consup map keyPath path.
   */
  protected String actualKey(String keyPath) {
    return keyPath.replace(name + "/", "");
  }

  /**
   * Returns NULL - safe key value list - simple wrapper around getting list out of {@link KeyValueList} instance.
   */
  List<KeyValue> nullSafeListResult(KeyValueList keyValueList) {
    return keyValueList == null || keyValueList.getList() == null ? Collections.emptyList() : keyValueList.getList();
  }

}
