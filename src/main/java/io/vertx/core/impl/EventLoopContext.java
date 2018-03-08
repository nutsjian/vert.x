/*
 * Copyright (c) 2011-2017 Contributors to the Eclipse Foundation
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
 * which is available at https://www.apache.org/licenses/LICENSE-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
 */

package io.vertx.core.impl;

import io.netty.channel.EventLoop;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
public class EventLoopContext extends ContextImpl {

  private static final Logger log = LoggerFactory.getLogger(EventLoopContext.class);

  public EventLoopContext(VertxInternal vertx, WorkerPool internalBlockingPool, WorkerPool workerPool, String deploymentID, JsonObject config,
                          ClassLoader tccl) {
    super(vertx, internalBlockingPool, workerPool, deploymentID, config, tccl);
  }

  public EventLoopContext(VertxInternal vertx, EventLoop eventLoop, WorkerPool internalBlockingPool, WorkerPool workerPool, String deploymentID, JsonObject config,
                          ClassLoader tccl) {
    super(vertx, eventLoop, internalBlockingPool, workerPool, deploymentID, config, tccl);
  }

  public void executeAsync(Handler<Void> task) {
    // No metrics, we are on the event loop.
    // 这里通过 wrapTask 方法把 handler 包装后提交给 eventLoop 执行
    nettyEventLoop().execute(wrapTask(null, task, true, null));
  }

  @Override
  public boolean isEventLoopContext() {
    return true;
  }

  @Override
  public boolean isMultiThreadedWorkerContext() {
    return false;
  }

  /**
   * 1. 判断当前线程是否是 VertxThread，这种线程是由 VertxThreadFactory 生成的
   * 2. 还会判断 context 的线程跟当前线程是否相同，可以保证 context 在其自己的线程中，这样也为了便于后面 runOnContext的时候
   * 保证执行的都在同一个线程中（线程安全）
   */
  @Override
  protected void checkCorrectThread() {
    Thread current = Thread.currentThread();
    if (!(current instanceof VertxThread)) {
      throw new IllegalStateException("Expected to be on Vert.x thread, but actually on: " + current);
    } else if (contextThread != null && current != contextThread) {
      throw new IllegalStateException("Event delivered on unexpected thread " + current + " expected: " + contextThread);
    }
  }

}
