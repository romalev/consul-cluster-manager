package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.impl.ClusterSerializable;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.KeyValueOptions;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Base64;

/**
 * Provides specific functionality for async clustering maps.
 *
 * @author Roman Levytskyi
 */
abstract class ConsulMap<K, V> {

    private static final Logger log = LoggerFactory.getLogger(ConsulMap.class);

    protected final Vertx vertx;
    protected final ConsulClient consulClient;
    protected final ConsulClientOptions consulClientOptions;
    protected final String name;
    protected final String sessionId;
    protected final boolean hasToBeLockedByConsulSession;
    protected final KeyValueOptions kvOptions;

    public ConsulMap(Vertx vertx, ConsulClient consulClient, ConsulClientOptions consulClientOptions, String name, String sessionId) {
        this.vertx = vertx;
        this.consulClient = consulClient;
        this.consulClientOptions = consulClientOptions;
        this.name = name;
        this.sessionId = sessionId;
        this.hasToBeLockedByConsulSession = true;
        this.kvOptions = hasToBeLockedByConsulSession ? new KeyValueOptions().setAcquireSession(sessionId) : null;
    }

    protected Future<Void> putValue(K k, V v) {
        log.trace("'{}' - trying to put KV: '{}'->'{}' CKV.", name, k, v);
        return assertKeyAndValueAreNotNull(k, v)
                .compose(aVoid -> encodeInFuture(v))
                .compose(value -> {
                    Future<Void> future = Future.future();
                    consulClient.putValueWithOptions(getConsulKey(name, k), value, kvOptions, resultHandler -> {
                        if (resultHandler.succeeded()) {
                            log.trace("'{}'- KV: '{}'->'{}' has been put to CKV.", name, k.toString(), v.toString());
                            future.complete();
                        } else {
                            log.error("'{}' - Can't put KV: '{}'->'{}' to CKV due to: '{}'", name, k.toString(), v.toString(), future.cause().toString());
                            future.fail(resultHandler.cause());
                        }
                    });
                    return future;
                });
    }

    protected Future<Void> removeValue(K k) {
        return assertKeyIsNotNull(k)
                .compose(aVoid -> {
                    Future<Void> future = Future.future();
                    consulClient.deleteValue(getConsulKey(name, k), resultHandler -> {
                        if (resultHandler.succeeded()) {
                            log.trace("'{}' - K: '{}' has been removed from CKV.", name, k.toString());
                            future.succeeded();
                        } else {
                            log.trace("'{}' - Can't delete K: '{}' from CKV due to: '{}'.", name, k.toString(), resultHandler.cause().toString());
                            future.fail(resultHandler.cause());
                        }
                    });
                    return future;
                });
    }


    /**
     * Verifies whether value is not null.
     */
    Future<Void> assertValueIsNotNull(Object value) {
        boolean result = value == null;
        if (result) return io.vertx.core.Future.failedFuture("Value can not be null.");
        else return Future.succeededFuture();
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
        else return io.vertx.core.Future.succeededFuture();
    }

    String getConsulKey(String name, K k) {
        return name + "/" + k.toString();
    }

    Future<String> encodeInFuture(Object object) {
        Future<String> future = Future.future();
        try {
            String result = encode(object);
            future.complete(result);
        } catch (IOException e) {
            future.fail(e.getCause());
        }
        return future;
    }

    String encode(Object object) throws IOException {
        StringBuilder encoded = new StringBuilder();
        encoded
                .append(new String(Base64.getEncoder().encode(asByte(object))))
                .append("--SEPARATOR--")
                .append(object.toString());
        return encoded.toString();
    }

    <T> T decode(String bytes) throws Exception {
        int index = bytes.lastIndexOf("--SEPARATOR--");
        String actualBytes = bytes.substring(0, index);
        return (T) asObject(Base64.getDecoder().decode(actualBytes.getBytes()));
    }

    /**
     * Marshals (encodes) an object to bytes[].
     */
    byte[] asByte(Object object) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutput dataOutput = new DataOutputStream(byteOut);
        if (object instanceof ClusterSerializable) {
            ClusterSerializable clusterSerializable = (ClusterSerializable) object;
            dataOutput.writeBoolean(true);
            dataOutput.writeUTF(object.getClass().getName());
            Buffer buffer = Buffer.buffer();
            clusterSerializable.writeToBuffer(buffer);
            byte[] bytes = buffer.getBytes();
            dataOutput.writeInt(bytes.length);
            dataOutput.write(bytes);
        } else {
            dataOutput.writeBoolean(false);
            ByteArrayOutputStream javaByteOut = new ByteArrayOutputStream();
            ObjectOutput objectOutput = new ObjectOutputStream(javaByteOut);
            objectOutput.writeObject(object);
            dataOutput.write(javaByteOut.toByteArray());
        }
        return byteOut.toByteArray();
    }

    /**
     * Unmarshals (decodes) bytes[] to object.
     */
    <T> T asObject(byte[] bytes) throws Exception {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(byteIn);
        boolean isClusterSerializable = in.readBoolean();
        if (isClusterSerializable) {
            String className = in.readUTF();
            Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            int length = in.readInt();
            byte[] body = new byte[length];
            in.readFully(body);
            try {
                ClusterSerializable clusterSerializable;
                //check clazz if have a public Constructor method.
                if (clazz.getConstructors().length == 0) {
                    Constructor<T> constructor = (Constructor<T>) clazz.getDeclaredConstructor();
                    constructor.setAccessible(true);
                    clusterSerializable = (ClusterSerializable) constructor.newInstance();
                } else {
                    clusterSerializable = (ClusterSerializable) clazz.newInstance();
                }
                clusterSerializable.readFromBuffer(0, Buffer.buffer(body));
                return (T) clusterSerializable;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load class " + e.getMessage(), e);
            }
        } else {
            byte[] body = new byte[in.available()];
            in.readFully(body);
            ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(body));
            return (T) objectIn.readObject();
        }
    }

}
