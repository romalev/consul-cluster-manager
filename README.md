# Consul cluster manager for Vert.x ecosystem #

**Introduction**
-
Consul - based cluster manager that is plugable into Vert.x ecosystem. **[Consul](https://www.consul.io/)** is a distributed, highly available, and data center aware solution to connect and configure applications across dynamic, distributed infrastructure. 

***Project status***

Project is still being under POC. Lost of application logging is still present.

***Motivation***

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

***Implementation details***

Current consul cluster manager implementation is fully based on [**vertx-consul-client**](https://vertx.io/docs/vertx-consul-client/java/) and [**vertx-core**](https://vertx.io/docs/vertx-core/java/).

***How to use***

```
// 1. Create an instance of ConsulClusterManager. ConsulClusterManager strictly relies on vertx-consul-client (https://vertx.io/docs/vertx-consul-client/java/) 
// so ConsulClientOptions can be used to adjust consul client configuration (ConsulClient is internally used).  
ConsulClusterManager consulClusterManager = new ConsulClusterManager(new ConsulClientOptions());
// or
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

***Restrictions***
- The limit on a key's value size of any of the consul maps is 512KB. This is strictly enforced and an HTTP 413 status will be returned to any client that attempts to store more than that limit in a value.  
- TTL value (on entries) must be between 10s and 86400s currently. [Invalidation-time is twice the TTL time](https://github.com/hashicorp/consul/issues/1172) - this means actual time when ttl entry gets removed (expired) is doubled to what you will specify as a ttl. 