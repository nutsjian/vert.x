package nutsjian.context;

import io.vertx.core.Vertx;

/**
 * executeBlocking() 方法用于执行阻塞任务，有两种模式：有序执行、无序执行
 */
public class ExecuteBlockingMethodExample {

  public static void main(String[] args) {
    // 实例化 VertxImpl 的时候 workerPool 已经创建了
    Vertx vertx = Vertx.vertx();

    /*
      vert.x-worker-thread-0
      vert.x-worker-thread-1
      vert.x-worker-thread-2
      vert.x-worker-thread-3
      vert.x-worker-thread-4
      vert.x-worker-thread-5

      提交任务的间隔是 1s，执行的时间是 0.2s，这样每次的 runner 就可以在下一个任务提交之前执行完，因此每次所用的线程会不同（轮询策略）
     */
//    vertx.setPeriodic(1000, t -> {
//      vertx.executeBlocking(future -> {
//        try {
//          Thread.sleep(200);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//        System.out.println(Thread.currentThread().getName());
//        future.complete();
//      }, r -> {});
//    });

    /*
        vert.x-worker-thread-0
        vert.x-worker-thread-0
        vert.x-worker-thread-0
        vert.x-worker-thread-0
        vert.x-worker-thread-0
        vert.x-worker-thread-0

        提交任务的间隔是 1s，而执行任务的时间是 2s，这样每次的 runner 无法在下一个任务提交之前执行完，
        因此，为了保证顺序，这些任务都应该在同一个 worker-thread 中运行，所以你看到的结果都是同一个线程

     */
//    vertx.setPeriodic(1000, t -> {
//      vertx.executeBlocking(future -> {
//        try {
//          Thread.sleep(2000);
//        } catch (InterruptedException e) {
//          e.printStackTrace();
//        }
//        System.out.println(Thread.currentThread().getName());
//        future.complete();
//      }, r -> {});
//    });

    /*
        vert.x-worker-thread-0
        vert.x-worker-thread-1
        vert.x-worker-thread-2
        vert.x-worker-thread-3
        vert.x-worker-thread-4
        vert.x-worker-thread-5

        提交任务的间隔是 1s，而执行任务的时间是 2s，这样每次的 runner 无法在下一个任务提交之前执行完，
        而且，当前 executeBlocking 指定运行无序，所以我们可以看到这些任务是在不同的 worker-thread 中运行的
     */
    vertx.setPeriodic(1000, t -> {
      vertx.executeBlocking(future -> {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName());
        future.complete();
      }, false, r -> {});
    });
  }

}

/*
* 上面两段代码分别执行为什么会有差异？
*
* 1. 正常情况下，executeBlocking 底层使用了 Worker 线程池，但是为什么两段代码执行时有这样的差异呢？
* 2. executeBlocking 方法默认 顺序执行 提交的阻塞任务。
*
* 先看下 Worker 线程池
* 1. Worker 线程池，是在创建 VertxImpl 实例的时候进行初始化的
*
*
*
*
*
*
*
*/
