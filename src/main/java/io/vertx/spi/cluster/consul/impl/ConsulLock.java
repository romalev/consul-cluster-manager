package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.KeyValueOptions;

import java.util.Optional;

import static io.vertx.spi.cluster.consul.impl.ClusterManagerMaps.VERTX_LOCKS;

/**
 * Consul-based implementation of an asynchronous exclusive lock which can be obtained from any node in the cluster.
 * When the lock is obtained (acquired), no-one else in the cluster can obtain the lock with the same name until the lock is released.
 * <p>
 * <b> Given implementation is based on using consul sessions - see https://www.consul.io/docs/guides/leader-election.html.</b>
 * Some notes:
 * <p>
 * The state of our lock would then correspond to the existence or non-existence of the respective key in the key-value store.
 * In order to acquire the lock we create a simple kv pair in Consul kv store and bound it with ttl session's id.
 * In order to release the lock we destroy respective ttl session which triggers automatically the deleting of kv pair that was bound to it.
 * <p>
 * Some additional details:
 * https://github.com/hashicorp/consul/issues/432
 * https://blog.emilienkenler.com/2017/05/20/distributed-locking-with-consul/
 *
 * @author Roman Levytskyi
 */
public class ConsulLock extends ConsulMap<String, String> implements Lock {

    private static final Logger log = LoggerFactory.getLogger(ConsulLock.class);

    private final String lockName;
    private Optional<String> sessionId = Optional.empty();

    public ConsulLock(String name, long timeout, ConsulClient consulClient) {
        super(VERTX_LOCKS.getName(), consulClient);
        this.lockName = name;
        acquire(timeout);
    }

    /**
     * Obtains the lock asynchronously.
     */
    private void acquire(long timeout) {
        getTtlSessionId(timeout, lockName)
                .compose(s -> {
                    sessionId = Optional.of(s);
                    return putConsulValue(getConsulKey(VERTX_LOCKS.getName(), lockName), "lockAcquired", new KeyValueOptions().setAcquireSession(s));
                })
                .setHandler(lockAcquiredRes -> {
                    if (lockAcquiredRes.result()) {
                        log.trace("Lock on: '{}'  has been acquired.", lockName);
                    } else {
                        sessionId = Optional.empty();
                        log.warn("Can't acquire lock on: '{}'. Someone else has already acquired it.");
                    }
                });
    }


    @Override
    public void release() {
        sessionId.ifPresent(s ->
                consulClient.destroySession(s, resultHandler -> {
                    if (resultHandler.succeeded()) log.trace("Lock: '{}' has been released.", lockName);
                    else
                        log.error("Can't release lock: '{}' due to: '{}'.", lockName, resultHandler.cause().toString());
                }));
    }
}