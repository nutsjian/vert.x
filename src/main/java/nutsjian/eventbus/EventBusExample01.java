package nutsjian.eventbus;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.impl.clustered.ClusteredEventBus;
import io.vertx.core.json.JsonObject;

public class EventBusExample01 {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    EventBus eventBus = vertx.eventBus();

//    vertx.setPeriodic(3000, h -> {
//      eventBus.send("test", new JsonObject().put("hello", "world"));
//    });

    vertx.setTimer(3000, h-> {
      eventBus.send("test", new JsonObject().put("hello", "world"));
    });

    eventBus.consumer("test", message -> {
      JsonObject json = (JsonObject) message.body();
      System.out.println(json.getString("hello"));
    });
  }

}

/*
 * EventBus 是 VertxImpl 的成员变量，并且是在 VertxImpl 的构造函数中初始化并启动的，所以每个 Vert.x Instance（实例）应该
 * 只有一个 EventBus 运行，并且通过该 Vertx 实例部署的 Verticle 可以通过该 EventBus 进行本地通信。
 *
 * 如果 Vertx 的启动方式是 Clustered，那么 EventBus 就变成一个集群模式的 EventBus。通信的时候会检查目标是否是本机，如果是
 * 本机，则直接进行本地通信。否则使用 TCP RPC 来通信。
 *
 * 1. 本地通信是如何进行的？
 * 2. Vert.x 实例部署的 Verticle 之间如何通过 EventBus 通信？
 *
 * vertx.eventBus();
 *  -> VertxImpl.eventBus();
 *
 *
 * <T> MessageConsumer<T> eventBus.consumer(String address);
 * <T> MessageConsumer<T> eventBus.consumer(String address, Handler<AsyncResult<T>> handler);
 *
 *  MessageConsumer <----------------> ( 1:1 ) Handler
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 *
 */
