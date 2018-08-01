package io.vertx.spi.cluster.consul.examples.testing;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.consul.ConsulClient;
import io.vertx.spi.cluster.consul.impl.ConsulCounter;

public class ConsulCounterTester {

    public static void main(String[] args) {

        Vertx vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(1));
        ConsulClient consulClient = ConsulClient.create(vertx);

        ConsulCounter consulCounter = new ConsulCounter("tester", consulClient);

        consulCounter.addAndGet(2L, event -> {
            System.out.println(event);
        });


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
