package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;

import java.io.*;
import java.lang.reflect.Constructor;

/**
 * Provides specific functionality for async clustering maps.
 *
 * @author Roman Levytskyi
 */
abstract class ConsulAbstractMap<K, V> {

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
