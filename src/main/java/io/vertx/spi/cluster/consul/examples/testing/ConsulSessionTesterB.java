package io.vertx.spi.cluster.consul.examples.testing;

import io.vertx.core.*;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.ext.consul.*;
import io.vertx.spi.cluster.consul.impl.AvailablePortFinder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class ConsulSessionTesterB {

    // slf4j
    static {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
    }

    private static final Logger log = LoggerFactory.getLogger(ConsulSessionTesterB.class);

    private String serviceName = UUID.randomUUID().toString();
    private Vertx vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(1));
    private int port = AvailablePortFinder.find(2000, 2010);
    private String host;

    private ConsulClientOptions consulClientOptions = new ConsulClientOptions();
    private ConsulClient consulClient = ConsulClient.create(vertx, consulClientOptions);

    {
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    private String checkId = "check-" + serviceName;

    public static void main(String[] args) throws InterruptedException {

        ConsulSessionTesterB tester = new ConsulSessionTesterB();

        tester.vertx.setPeriodic(15000, event -> {
            tester.consulClient.localChecks(localChecks -> {
                List<Check> failedCheck = localChecks.result().stream().filter(check -> check.getStatus() == CheckStatus.CRITICAL).collect(Collectors.toList());
                failedCheck.forEach(check -> {
                    tester.consulClient.deregisterCheck(check.getId(), checkDerRes -> {
                        if (checkDerRes.succeeded()) {
                            log.trace("Check: {} has been unregistered.", check.getId());
                        }
                    });
                });
            });
        });

        tester.run(event -> {

        });

    }

    public void run(Handler<AsyncResult<Void>> resultHandler) {

        registerCheck()
                .compose(aVoid -> createTcpServer())
                .compose(aBoolean -> registerSession())
                .compose(this::putSmthWithinConsulMap)
                .setHandler(resultHandler);
    }


    private Future<Void> wath() {
        Future<Void> future = Future.future();
        Watch.keyPrefix("vertx", vertx).setHandler(event -> {
            if (event.succeeded()) {
                if (event.nextResult() != null) {
                    log.trace("watch next: {}", event.nextResult().toJson().toString());
                }
                if (event.prevResult() != null) {
                    log.trace("watch prev: {}", event.prevResult().toJson().toString());
                }
            } else {
                log.error("Watch failed: {}", event.cause().toString());
            }
        }).start();
        future.complete();
        return future;
    }

    private Future<Void> createTcpServer() {
        Future<Void> future = Future.future();
        NetServer netServer = vertx.createNetServer(new NetServerOptions().setHost(host).setPort(port));
        netServer.connectHandler(event -> log.trace("Health heart beat message sent back to Consul"));
        netServer.listen(listenEvent -> {
            if (listenEvent.succeeded()) future.complete();
            else future.fail(listenEvent.cause());
        });

        return future;
    }

    private Future<Void> registerCheck() {
        Future<Void> future = Future.future();


        CheckOptions checkOptions = new CheckOptions();
        checkOptions.setName(checkId);
        checkOptions.setId(checkId);
        checkOptions.setTcp(host + ":" + port);
        checkOptions.setInterval("5s");
        checkOptions.setStatus(CheckStatus.PASSING);


        consulClient.registerCheck(checkOptions, res -> {
            if (res.succeeded()) {
                log.trace("Check has been registered : '{}'", checkOptions.getId());
                future.complete();
            } else {
                log.trace("Can't register check: '{}' due to: '{}'", checkOptions.getId(), res.cause().toString());
                future.fail(res.cause());
            }
        });

        return future;
    }

    private Future<String> registerSession() {
        Future<String> future = Future.future();
        SessionOptions sessionOptions = new SessionOptions();
        sessionOptions.setBehavior(SessionBehavior.DELETE);
        sessionOptions.setName(serviceName);
        //sessionOptions.setNode("WP2491");


        sessionOptions.setChecks(Arrays.asList(checkId, "serfHealth"));
        consulClient.createSessionWithOptions(sessionOptions, res -> {
            if (res.succeeded()) {
                log.trace("Session : '{}' has been registered.", res.result());
                future.complete(res.result());
            } else {
                log.error("Couldn't register the session due to: {}", res.cause().toString());
                future.fail(res.cause());
            }
        });
        return future;
    }

    private Future<Void> putSmthWithinConsulMap(String sessionId) {

        KeyValueOptions keyValueOptions = new KeyValueOptions();
        keyValueOptions.setAcquireSession(sessionId);

        Future<Void> future = Future.future();
        String key = "vertx/" + "keyB";
        String value = "service B";

        consulClient.putValueWithOptions(key, value, keyValueOptions, res -> {
            if (res.succeeded()) {
                log.trace("KV: '{}'->'{}' has been placed within consul map. Acquiring the lock on this key by session : {}", key, value, sessionId);

                future.complete();
            } else {
                log.error("Can't place KV: '{}'->'{}' into consul map due to: {}", key, value, res.cause().toString());
                future.fail(res.cause());
            }
        });

        return future;
    }


}