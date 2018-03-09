package nutsjian.deploy;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.impl.EventLoopContext;

public class DeployExample01 {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();

    // 没有指定 poolName 等信息，会使用 VertxImpl 内部创建好的 workerPool 线程池
    DeploymentOptions options = new DeploymentOptions();
    options.setInstances(4);
    vertx.deployVerticle(AAAVerticle::new, options, ar -> {
      // 返回 deploymentId
      System.out.println(ar.result());
    });

    // 这里配置了 poolName 等信息，下面部署的 verticle 会使用自定以的那个 workerPool 线程池
    DeploymentOptions options1 = new DeploymentOptions();
    options1.setWorkerPoolName("hello-worker-pool");
    options1.setWorkerPoolSize(10);
    options1.setMaxWorkerExecuteTime(1000);
    vertx.deployVerticle(BBBVerticle::new, options1);

    // 分析 卸载 verticle
  }

}
