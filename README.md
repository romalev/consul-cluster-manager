# Consul cluster manager for Vert.x ecosystem #

[![Build Status](https://travis-ci.com/romalev/vertx-consul-cluster-manager.svg?branch=master)](https://travis-ci.com/romalev/vertx-consul-cluster-manager)

**Introduction**
-
Consul - based cluster manager that is plugable into Vert.x ecosystem. **[Consul](https://www.consul.io/)** is a distributed, highly available, and data center aware solution to connect and configure applications across dynamic, distributed infrastructure. 

**Project status**
-
Project is still being under POC. Vert.x core TCK is passing. Your feedback would be greatly appreciated.

**Motivation**
- 
As we all know Vert.x cluster managers are pluggable and so far we have 6 cluster manager implementations: 

- Hazelcast - based: https://vertx.io/docs/vertx-hazelcast/java/
- Apache Zookeeper - based: https://vertx.io/docs/vertx-zookeeper/java/  
- Apache Ignite - based: https://vertx.io/docs/vertx-ignite/java/
- JGroups - based: https://github.com/vert-x3/vertx-jgroups
- Atomix - based : https://github.com/atomix/atomix-vertx
- Infinispan - based : https://github.com/vert-x3/vertx-infinispan 

If you are already using Consul along with Vert.x within your system - it might be a good fit to use Consul directly as main manager for : 
- discovery and group membership of Vert.x nodes in a cluster
Maintaining cluster wide topic subscriber lists (so we know which nodes are interested in which event bus addresses)
- distributed map support;
- distributed locks;
- distributed counters;   

Note : Cluster managers do not handle the event bus inter-node transport, this is done directly by Vert.x with TCP connections.

**Implementation details**
-
Current consul cluster manager implementation is fully based on [**vertx-consul-client**](https://vertx.io/docs/vertx-consul-client/java/) and [**vertx-core**](https://vertx.io/docs/vertx-core/java/).

**How to use**
-

### Gradle
```groovy

repositories {
    //...
    maven { url 'https://jitpack.io' }
}

compile 'com.github.romalev:vertx-consul-cluster-manager:v0.0.7-beta'
```

### Maven
```xml

<project>
  <repositories>
    <repository>
      <id>jitpack</id>
      <url>https://jitpack.io</url>
    </repository>
  </repositories>
</project>

<dependency>
  <groupId>com.github.romalev</groupId>
  <artifactId>vertx-consul-cluster-manager</artifactId>
  <version>v0.0.7-beta</version>
</dependency>
```

```
// 1. Create an instance of ConsulClusterManager. ConsulClusterManager strictly relies on vertx-consul-client (https://vertx.io/docs/vertx-consul-client/java/) 
JsonObject options = new JsonObject()
 // host where consul agent is running, if not specified default host will be used which is "localhost"
 .put("host", "localhost") 
 // port on wich consul agent is listening, if not specified default port will be used which is "8500"
 .put("port", consulAgentPort) 
 // * false - enable internal caching of event bus subscribers - this will give us better latency but stale reads (stale subsribers) might appear.
 // * true - disable internal caching of event bus subscribers - this will give us stronger consistency in terms of fetching event bus subscribers, 
 // but this will result in having much more round trips to consul kv store where event bus subs are being kept.
 .put("preferConsistency", true);   
ConsulClusterManager consulClusterManager = new ConsulClusterManager(options);
// or use defaults: 
// ConsulClusterManager consulClusterManager = new ConsulClusterManager();

// 2. Create VertxOptions instance and specify consul cluster manager.
VertxOptions vertxOptions = new VertxOptions();
vertxOptions.setClusterManager(consulClusterManager);
Vertx.clusteredVertx(vertxOptions, res -> {
    if (res.succeeded()) {
	    // clustered vertx instance has been successfully created!
	    Vertx vertx = res.result(); 
	} else {
	    // something went wrong :( 
	}
}
```

**Restrictions**
-
- Compatible with **ONLY Vert.x 3.6.+** release.
- The limit on a key's value size of any of the consul  maps is 512KB.
