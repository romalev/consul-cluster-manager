package io.vertx.spi.cluster.consul.examples;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.consul.ServiceList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// has to be rewritten
public class ServiceB {

    public static void main(String[] args) {
        FakeConsulService service = new FakeConsulService();

        Future<List<String>> future = Future.future();

        service.catalogServices(event -> {
            if (event.succeeded()) {
                System.out.println(event.result());
                future.complete(event.result());
            }
        });

        System.out.println("avalilabel services : " + future.result());
    }

    static class FakeConsulService {
        void catalogServices(Handler<AsyncResult<List<String>>> resultHandler) {
            // simulate some delay...
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            resultHandler.handle(Future.succeededFuture(Arrays.asList("serviceA")));
        }
    }
}
