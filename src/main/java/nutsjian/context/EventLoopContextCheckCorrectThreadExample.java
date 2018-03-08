package nutsjian.context;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxThread;

public class EventLoopContextCheckCorrectThreadExample {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    // 没有在 Verticle 中，所以这里自行创建了一个 EventLoopContext
    Context context = vertx.getOrCreateContext();

    System.out.println(context);
    System.out.println(context.isEventLoopContext());
    System.out.println(context.isMultiThreadedWorkerContext());
    System.out.println(context.isWorkerContext());
    System.out.println(context.owner());

    // 由于当前线程是 main 线程，不是 VertxThread，所以会报错。
    // 这个方法在 EventLoopContext 中有实现，是为了检查 EventLoopContext 是否在 VertxThread 中，还会判断是否是当前线程，具体
    // 可以看EventLoopContext.checkCorrectContext
    Thread current = Thread.currentThread();
    if (!(current instanceof VertxThread)) {
      throw new IllegalStateException("Expected to be on Vert.x thread, but actually on: " + current);
    }
  }
}
