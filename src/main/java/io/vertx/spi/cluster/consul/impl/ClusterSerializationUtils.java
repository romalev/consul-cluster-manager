package io.vertx.spi.cluster.consul.impl;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.impl.ClusterSerializable;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Base64;

/**
 * Dedicated utility to marshal objects into strings and un-marshal strings into objects.
 *
 * @author Roman Levytskyi.
 */
public class ClusterSerializationUtils {

    static Future<String> encodeF(Object object) {
        Future<String> future = Future.future();
        try {
            String result = encode(object);
            future.complete(result);
        } catch (IOException e) {
            future.fail(e.getCause());
        }
        return future;
    }

    static <T> Future<T> decodeF(String bytes) {
        Future<T> future = Future.future();
        try {
            if (bytes == null) {
                future.complete();
            } else {
                future.complete(decode(bytes));
            }
        } catch (Exception e) {
            future.fail(e);
        }
        return future;
    }

    static String encode(Object object) throws IOException {
        StringBuilder encoded = new StringBuilder();
        encoded
                .append(new String(Base64.getEncoder().encode(asByte(object))))
                .append("--SEPARATOR--")
                .append(object.toString());
        return encoded.toString();
    }

    public static <T> T decode(String bytes) throws Exception {
        int index = bytes.lastIndexOf("--SEPARATOR--");
        String actualBytes = bytes.substring(0, index);
        return (T) asObject(Base64.getDecoder().decode(actualBytes.getBytes()));
    }

    /**
     * Marshals (encodes) an object to bytes[].
     */
    static private byte[] asByte(Object object) throws IOException {
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
    static private <T> T asObject(byte[] bytes) throws Exception {
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
