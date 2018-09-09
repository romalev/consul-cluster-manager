package io.vertx.spi.cluster.consul.impl;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CacheMultiMap<V> {

    private ConcurrentMap<String, ChoosableSet<V>> cache = new ConcurrentHashMap<>();

}
