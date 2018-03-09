package nutsjian.deploy;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.EventLoopContext;

public class DeployExample01 {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    DeploymentOptions options = new DeploymentOptions();
    options.setInstances(4);
    vertx.deployVerticle(AAAVerticle::new, options);
  }

}
