package io.vertx.spi.cluster.consul.examples.testing;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.ext.consul.ConsulClientOptions;
import io.vertx.ext.consul.SessionBehavior;
import io.vertx.ext.consul.SessionOptions;
import io.vertx.spi.cluster.consul.impl.ConsulAsyncMap;

import java.util.concurrent.TimeUnit;

public class ConsulCounterTester {

    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(1));
        ConsulClient consulClient = ConsulClient.create(vertx);

        String consulKey = "abb";
        String sessionName = "ttlSession_" + consulKey;

        SessionOptions ttlSession = new SessionOptions()
                .setTtl(TimeUnit.MILLISECONDS.toSeconds(13000))
                .setBehavior(SessionBehavior.DELETE)
                .setName(sessionName);


        ConsulAsyncMap consulAsyncMap = new ConsulAsyncMap("__async", vertx, consulClient, new ConsulClientOptions());
        consulAsyncMap.putIfAbsent("test", "test2", handler -> {
            System.out.println(handler);
        });




//        ConsulCounter consulCounter = new ConsulCounter("tester", consulClient);
//
//        consulCounter.addAndGet(2L, event -> {
//            System.out.println(event);
//        });


        // Specifies to use a Check-And-Set operation :if the index is 0, Consul will only put the key if it does not already exist.
//        KeyValueOptions casOptiosn = new KeyValueOptions().setCasIndex(0);
//        consulClient.putValueWithOptions("key", "value", casOptiosn, result -> {
//            System.out.println(result.result());
//            if (result.succeeded() && result.result()) {
//                System.out.println("succeded.");
//            } else {
//                System.out.println("failed");
//            }
//        });

    }
}
