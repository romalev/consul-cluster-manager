package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
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
 * @See {@link ConsulEntry}
 */
class ConversationUtils {

  private static final String KEY_TAG = "KEY";
  private static final String VALUE_TAG = "VALUE";
  private static final String NODE_ID_TAG = "NODE_ID";

  private static final String INFO_TAG = "INFO";
  private static final String SEPARATOR = "ENCODED_VALUE";

  static <K, V> String asString(K key, V value, String nodeId) throws IOException {
    JsonObject jsonObject = new JsonObject()
      .put(KEY_TAG, asString(key))
      .put(VALUE_TAG, asString(value))
      .put(NODE_ID_TAG, nodeId)
      .put(INFO_TAG, value.toString());
    return jsonObject.encodePrettily();
  }

  static <K, V> ConsulEntry<K, V> asConsulEntry(String consulEntry) throws Exception {
    JsonObject jsonObject = new JsonObject(consulEntry);
    K key = asObject(jsonObject.getString(KEY_TAG));
    V value = asObject(jsonObject.getString(VALUE_TAG));
    String nodeId = jsonObject.getString(NODE_ID_TAG);
    return new ConsulEntry<>(key, value, nodeId);
  }

  static <K, V> Future<String> asFutureString(K key, V value, String nodeId) {
    Future<String> result = Future.future();
    try {
      result.complete(asString(key, value, nodeId));
    } catch (IOException e) {
      result.fail(e);
    }
    return result;
  }

  static <K, V> Future<ConsulEntry<K, V>> asFutureConsulEntry(String object) {
    Future<ConsulEntry<K, V>> result = Future.future();
    if (object == null) result.complete();
    else {
      try {
        result.complete(asConsulEntry(object));
      } catch (Exception e) {
        result.fail(e);
      }
    }
    return result;
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
}
