package nutsjian.eventbus;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;

public class EventBusExample01 {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    EventBus eventBus = vertx.eventBus();
  }

}

/*
 * vertx.eventBus();
 *  -> VertxImpl.eventBus();
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
