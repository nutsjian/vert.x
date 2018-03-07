package nutsjian.eventbus;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

// 需要 vertx-hazelcast 库依赖
public class ClusteredEventBusExample01 {
  public static void main(String[] args) {

    VertxOptions options = new VertxOptions();
    Vertx.clusteredVertx(options, res -> {
      if (res.succeeded()) {
        Vertx vertx = res.result();
        EventBus eventBus = vertx.eventBus();
        System.out.println("we now have a clustered event bus: " +  eventBus);

        vertx.setTimer(3000, h-> {
          eventBus.send("test", new JsonObject().put("hello", "world"));
        });

        eventBus.consumer("test", message -> {
          JsonObject json = (JsonObject) message.body();
          System.out.println(json.getString("hello"));
        });
      } else {
        System.out.println("vertx start failed");
      }
    });
  }
}

/*
* ClusteredEventBus extends EventBusImpl
*
*
*
*
*
*
*/
