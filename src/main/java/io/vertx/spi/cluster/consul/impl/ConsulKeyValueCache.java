package io.vertx.spi.cluster.consul.impl;

/**
 * A utility that attempts to keep all data from all created Consul MAPS locally cached. This class
 * will watch the every consul map, respond to update/create/delete events, pull down the data, etc.
 * <p>
 * TODO : implement this.
 * <p>
 * Correlations : node(sessionId) -> maps.
 */
public class ConsulKeyValueCache {

    // We need a way to keep all the KV data cached locally.

}
