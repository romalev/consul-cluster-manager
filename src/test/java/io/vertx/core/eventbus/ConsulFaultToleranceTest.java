//package io.vertx.core.eventbus;
//
//import io.vertx.core.spi.cluster.ClusterManager;
//import io.vertx.ext.consul.ConsulClientOptions;
//import io.vertx.spi.cluster.consul.ConsulClusterManager;
// FIXME:
///**
// * The -conf argument does not point to an existing file or is not a valid JSON object
// * io.vertx.core.json.DecodeException: Failed to decode: Unexpected character ('i' (code 105)): was expecting double-quote to start field name
// * at [Source: (String)"{id:1,addressesCount:10}"; line: 1, column: 3]
// * at io.vertx.core.json.Json.decodeValue(Json.java:120)
// * at io.vertx.core.json.JsonObject.fromJson(JsonObject.java:956)
// * at io.vertx.core.json.JsonObject.<init>(JsonObject.java:48)
// * at io.vertx.core.impl.launcher.commands.BareCommand.getJsonFromFileOrString(BareCommand.java:259)
// * at io.vertx.core.impl.launcher.commands.RunCommand.getConfiguration(RunCommand.java:414)
// * at io.vertx.core.impl.launcher.commands.RunCommand.run(RunCommand.java:243)
// * at io.vertx.core.impl.launcher.VertxCommandLauncher.execute(VertxCommandLauncher.java:226)
// * at io.vertx.core.impl.launcher.VertxCommandLauncher.dispatch(VertxCommandLauncher.java:361)
// * at io.vertx.core.impl.launcher.VertxCommandLauncher.dispatch(VertxCommandLauncher.java:324)
// * at io.vertx.core.Launcher.main(Launcher.java:45)
// */
//public class ConsulFaultToleranceTest extends FaultToleranceTest {
//
//  @Override
//  protected ClusterManager getClusterManager() {
//    return new ConsulClusterManager(new ConsulClientOptions(), true);
//  }
//
//}
