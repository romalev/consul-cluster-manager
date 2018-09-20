package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Base64;

/**
 * Dedicated utility to marshal objects into strings and un-marshal strings into objects.
 * Consul client doesn't support writing byte array to consul KV store - the only thing we have is put(String key, String value).
 * In order to address the support of being able to save different types of java objects we first
 * 1) serialize objects and get their byte array.
 * 2) base64 encode this byte array - as a result we receive a string value.
 *
 * @author Roman Levytskyi.
 */
public class ConversationUtils {

    private static final String SEPARATOR = "--SEPARATOR--";


    public static <K, V> Future<String> encodeF(K key, V value) {
        Future<String> result = Future.future();
        try {
            result.complete(encode(key, value));
        } catch (IOException e) {
            result.fail(e);
        }
        return result;
    }

    public static <K, V> String encode(K key, V value) throws IOException {
        String encodedKey = asString(key);
        String encodedValue = asString(value);
        return encodedKey + SEPARATOR + encodedValue + SEPARATOR + key + "->" + value;
    }

    public static <K, V> Future<GenericEntry<K, V>> decodeF(String object) {
        Future<GenericEntry<K, V>> result = Future.future();
        if (object == null) result.complete();
        else {
            try {
                result.complete(decode(object));
            } catch (Exception e) {
                result.fail(e);
            }
        }
        return result;
    }

    public static <K, V> GenericEntry<K, V> decode(String object) throws Exception {
        if (object == null) {
            throw new VertxException("Can't decode a null object.");
        }
        String key = object.substring(0, object.indexOf(SEPARATOR));
        String value = object.substring(object.indexOf(SEPARATOR) + SEPARATOR.length(), object.lastIndexOf(SEPARATOR));
        K decodedKey = asObject(key);
        V decodedValue = asObject(value);
        return new GenericEntry(decodedKey, decodedValue);
    }


    private static String asString(Object object) throws IOException {
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
        return Base64.getEncoder().encodeToString(byteOut.toByteArray());
    }


    private static <T> T asObject(String object) throws Exception {
        final byte[] data = Base64.getDecoder().decode(object);
        ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
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

    public static class GenericEntry<K, V> implements Serializable {
        private K key;
        private V value;

        public GenericEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }


        @Override
        public String toString() {
            return "Entry{" +
                    "key=" + key.toString() +
                    ", value=" + value.toString() +
                    '}';
        }
    }

    // TODO: has to be removed.
//    public static void main(String[] args) throws Exception {
//        Integer key = 0;
//        String value = "roman";
//
//        encodeF(key, value)
//                .compose(s -> {
//                    System.out.println(s);
//                    return Future.succeededFuture(s);
//                }).compose(ConversationUtils::decodeF)
//                .setHandler(event -> {
//                    if (event.succeeded()) {
//                        System.out.println(event.result());
//                    } else {
//                        System.out.println(event.cause());
//                    }
//                });
//    }
}
