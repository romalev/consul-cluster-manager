package io.vertx.spi.cluster.consul.impl;

/**
 * Holds names of maps being used in doing cluster management
 *
 * @author Roman Levytskyi
 */
enum ClusterManagerMaps {

    // contains ha info about every available node within the cluster.
    VERTX_HA_INFO("__vertx.haInfo"),
    // contains subscribers listening for events on event bus channels.
    VERTX_SUBS("__vertx.subs");


    private String name;

    ClusterManagerMaps(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ClusterManagerMaps fromString(String text) {
        for (ClusterManagerMaps b : ClusterManagerMaps.values()) {
            if (text.equals(b.name)) {
                return b;
            }
        }
        return null;
    }
}
