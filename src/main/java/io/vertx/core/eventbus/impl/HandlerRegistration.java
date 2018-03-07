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

package io.vertx.core.eventbus.impl;

import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.impl.Arguments;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.metrics.EventBusMetrics;
import io.vertx.core.streams.ReadStream;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.Queue;

/*
 * This class is optimised for performance when used on the same event loop it was created on.
 * However it can be used safely from other threads.
 *
 * The internal state is protected using the synchronized keyword. If always used on the same event loop, then
 * we benefit from biased locking which makes the overhead of synchronized near zero.
 */
public class HandlerRegistration<T> implements MessageConsumer<T>, Handler<Message<T>> {

  private static final Logger log = LoggerFactory.getLogger(HandlerRegistration.class);

  public static final int DEFAULT_MAX_BUFFERED_MESSAGES = 1000;

  private final Vertx vertx;
  private final EventBusMetrics metrics;
  private final EventBusImpl eventBus;
  private final String address;
  private final String repliedAddress;
  private final boolean localOnly;
  private final Handler<AsyncResult<Message<T>>> asyncResultHandler;
  private long timeoutID = -1;
  private boolean registered;
  private Handler<Message<T>> handler;
  private Context handlerContext;
  private AsyncResult<Void> result;
  private Handler<AsyncResult<Void>> completionHandler;
  private Handler<Void> endHandler;
  private Handler<Message<T>> discardHandler;
  private int maxBufferedMessages = DEFAULT_MAX_BUFFERED_MESSAGES;
  private final Queue<Message<T>> pending = new ArrayDeque<>(8);
  private boolean paused;
  private Object metric;

  public HandlerRegistration(Vertx vertx, EventBusMetrics metrics, EventBusImpl eventBus, String address,
                             String repliedAddress, boolean localOnly,
                             Handler<AsyncResult<Message<T>>> asyncResultHandler, long timeout) {
    this.vertx = vertx;
    this.metrics = metrics;
    this.eventBus = eventBus;
    this.address = address;
    this.repliedAddress = repliedAddress;
    this.localOnly = localOnly;
    this.asyncResultHandler = asyncResultHandler;
    // TODO 待分析 EventBus Local 源码分析
    if (timeout != -1) {
      timeoutID = vertx.setTimer(timeout, tid -> {
        if (metrics != null) {
          metrics.replyFailure(address, ReplyFailure.TIMEOUT);
        }
        sendAsyncResultFailure(ReplyFailure.TIMEOUT, "Timed out after waiting " + timeout + "(ms) for a reply. address: " + address + ", repliedAddress: " + repliedAddress);
      });
    }
  }

  @Override
  public synchronized MessageConsumer<T> setMaxBufferedMessages(int maxBufferedMessages) {
    Arguments.require(maxBufferedMessages >= 0, "Max buffered messages cannot be negative");
    while (pending.size() > maxBufferedMessages) {
      pending.poll();
    }
    this.maxBufferedMessages = maxBufferedMessages;
    return this;
  }

  @Override
  public synchronized int getMaxBufferedMessages() {
    return maxBufferedMessages;
  }

  @Override
  public String address() {
    return address;
  }

  @Override
  public synchronized void completionHandler(Handler<AsyncResult<Void>> completionHandler) {
    Objects.requireNonNull(completionHandler);
    if (result != null) {
      AsyncResult<Void> value = result;
      vertx.runOnContext(v -> completionHandler.handle(value));
    } else {
      this.completionHandler = completionHandler;
    }
  }

  @Override
  public synchronized void unregister() {
    doUnregister(null);
  }

  @Override
  public synchronized void unregister(Handler<AsyncResult<Void>> completionHandler) {
    Objects.requireNonNull(completionHandler);
    doUnregister(completionHandler);
  }

  public void sendAsyncResultFailure(ReplyFailure failure, String msg) {
    unregister();
    asyncResultHandler.handle(Future.failedFuture(new ReplyException(failure, msg)));
  }

  // 取消消息处理函数的注册
  private void doUnregister(Handler<AsyncResult<Void>> completionHandler) {
    // 取消定时器
    if (timeoutID != -1) {
      vertx.cancelTimer(timeoutID);
    }
    if (endHandler != null) {
      Handler<Void> theEndHandler = endHandler;
      Handler<AsyncResult<Void>> handler = completionHandler;
      completionHandler = ar -> {
        theEndHandler.handle(null);
        if (handler != null) {
          handler.handle(ar);
        }
      };
    }
    if (registered) {
      registered = false;
      eventBus.removeRegistration(address, this, completionHandler);
    } else {
      callCompletionHandlerAsync(completionHandler);
    }
  }

  private void callCompletionHandlerAsync(Handler<AsyncResult<Void>> completionHandler) {
    if (completionHandler != null) {
      vertx.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
    }
  }

  synchronized void setHandlerContext(Context context) {
    handlerContext = context;
  }

  public synchronized void setResult(AsyncResult<Void> result) {
    this.result = result;
    if (completionHandler != null) {
      if (metrics != null && result.succeeded()) {
        metric = metrics.handlerRegistered(address, repliedAddress);
      }
      Handler<AsyncResult<Void>> callback = completionHandler;
      vertx.runOnContext(v -> callback.handle(result));
    } else if (result.failed()) {
      log.error("Failed to propagate registration for handler " + handler + " and address " + address);
    } else if (metrics != null) {
      metric = metrics.handlerRegistered(address, repliedAddress);
    }
  }

  /**
   * 处理消息的函数
   *
   * HandlerRegistration 实现了 MessageConsumer，而 MessageConsumer 接口又继承 ReadStream 接口，这就需要实现 flow control（back-pressure）的相关逻辑。
   * 如何实现呢？
   *    HandlerRegistration 中有一个 paused 标志位代表是否还继续处理消息。ReadStream 接口中定义了两个函数用于控制 stream 的通断。
   *    当处理速度 小于 读取速度（发生拥塞）的时候，我们可以通过 pause() 方法暂停消息的传递，将积压的消息暂存于内部的消息队列（缓冲区） pendding 中，
   *    当相对速度正常的时候，我们可以通过 resume() 方法恢复消息的传递和处理。
   */
  @Override
  public void handle(Message<T> message) {
    Handler<Message<T>> theHandler;
    // 为了防止资源挣用，加上 synchronized 加锁
    synchronized (this) {
      // 首先判断当前的 consumer 是否为 paused 状态
      if (paused) { // 暂停状态
        // 如果 paused = true，再检查当前缓冲区大小是否已经超过给定的最大缓冲区大小？
        if (pending.size() < maxBufferedMessages) {
          // 没有超过，将收到的消息加入到缓冲区
          pending.add(message);
        } else {
          // 如果大于或等于这个阈值 并且 存在 discardHandler，Vert.x 就需要丢弃超出的那部分消息（通过 discardHandler 处理器）
          if (discardHandler != null) {
            discardHandler.handle(message);
          } else {
            // 如果大于或等于这个阈值，但是不存在 discardHandler，则打印警告信息
            log.warn("Discarding message as more than " + maxBufferedMessages + " buffered in paused consumer. address: " + address);
          }
        }
        // 直接退出，不处理消息（当前是暂停状态）
        return;
      } else {
        // 当前 consumer 为正常状态
        if (pending.size() > 0) {
          // 缓冲区不为空，将收到的消息 push 到缓冲区中
          // 并且从缓冲区中 poll 队列首端的消息
          pending.add(message);
          message = pending.poll();
        }
        theHandler = handler;
      }
    }
    // 最后调用 deliver 处理消息
    // 注意这里是锁之外处理的消息，这是为了保证 multithreaded worker context 下可以并发传递消息
    // 由于 multithreaded worker context 允许在不同线程中并发执行逻辑，如果把 deliver 方法置于 synchronized 块之内
    // 其它线程必须等待当前锁被释放才能进行消息的传递逻辑，因而不能做到 delivery concurrently
    // bug https://bugs.eclipse.org/bugs/show_bug.cgi?id=473714
    deliver(theHandler, message);
  }

  private void deliver(Handler<Message<T>> theHandler, Message<T> message) {
    // Handle the message outside the sync block
    // https://bugs.eclipse.org/bugs/show_bug.cgi?id=473714
    // 首先 vert.x 会调用 checkNextTick 方法来检查消息队列（缓冲区）中是否存在更多的消息等待被处理。
    checkNextTick();

    // 检查消息是否是 ClusteredMessage，并标记 local
    boolean local = true;
    if (message instanceof ClusteredMessage) {
      // A bit hacky
      ClusteredMessage cmsg = (ClusteredMessage)message;
      if (cmsg.isFromWire()) {
        local = false;
      }
    }

    // 这个 creditsAddress 是什么？
    // 答案：MessageProducer 接口对应某个 address 上的消息生产者，同时它继承了 WriteStream 接口，因此
    // MessageProducer 的实现类 MessageProducerImpl 同样具有 flow control 的能力。我们可以把 MessageProducer 看成
    // 是一个具有 flow control 功能的增强版 EventBus。我们可以通过 EventBus 接口的publisher 方法创建一个 MessageProducer
    String creditsAddress = message.headers().get(MessageProducerImpl.CREDIT_ADDRESS_HEADER_NAME);
    if (creditsAddress != null) {
      eventBus.send(creditsAddress, 1);
    }
    try {
      // 处理消息前，记录 metric 数据
      if (metrics != null) {
        metrics.beginHandleMessage(metric, local);
      }
      // 处理消息
      theHandler.handle(message);

      // 处理消息后，记录 metric 数据
      if (metrics != null) {
        metrics.endHandleMessage(metric, null);
      }
    } catch (Exception e) {
      log.error("Failed to handleMessage. address: " + message.address(), e);
      if (metrics != null) {
        metrics.endHandleMessage(metric, e);
      }
      throw e;
    }
  }

  private synchronized void checkNextTick() {
    // Check if there are more pending messages in the queue that can be processed next time around
    if (!pending.isEmpty()) {
      handlerContext.runOnContext(v -> {
        Message<T> message;
        Handler<Message<T>> theHandler;
        synchronized (HandlerRegistration.this) {
          if (paused || (message = pending.poll()) == null) {
            return;
          }
          theHandler = handler;
        }
        deliver(theHandler, message);
      });
    }
  }

  /*
   * Internal API for testing purposes.
   */
  public synchronized void discardHandler(Handler<Message<T>> handler) {
    this.discardHandler = handler;
  }

  /**
   * 绑定消息处理函数
   *
   * 一个 MessageConsumer 对应一个消息处理函数。
   * @param handler 消息处理函数
   */
  @Override
  public synchronized MessageConsumer<T> handler(Handler<Message<T>> handler) {
    // 设置为成员变量
    this.handler = handler;
    // 消息处理函数不为空 && 还没注册该函数
    if (this.handler != null && !registered) {
      // 注册并标记
      registered = true;
      // 调用 eventBus 的 addRegistration 把此 consumer 注册到 EventBus 上
      eventBus.addRegistration(address, this, repliedAddress != null, localOnly);
    } else if (this.handler == null && registered) {
      // 消息处理函数为空 && 已经注册
      // 则调用 unregister 取消注册
      // This will set registered to false
      this.unregister();
    }
    return this;
  }

  @Override
  public ReadStream<T> bodyStream() {
    return new BodyReadStream<>(this);
  }

  @Override
  public synchronized boolean isRegistered() {
    return registered;
  }

  @Override
  public synchronized MessageConsumer<T> pause() {
    if (!paused) {
      paused = true;
    }
    return this;
  }

  @Override
  public synchronized MessageConsumer<T> resume() {
    if (paused) {
      paused = false;
      checkNextTick();
    }
    return this;
  }

  @Override
  public synchronized MessageConsumer<T> endHandler(Handler<Void> endHandler) {
    if (endHandler != null) {
      // We should use the HandlerHolder context to properly do this (needs small refactoring)
      Context endCtx = vertx.getOrCreateContext();
      this.endHandler = v1 -> endCtx.runOnContext(v2 -> endHandler.handle(null));
    } else {
      this.endHandler = null;
    }
    return this;
  }

  @Override
  public synchronized MessageConsumer<T> exceptionHandler(Handler<Throwable> handler) {
    return this;
  }

  public Handler<Message<T>> getHandler() {
    return handler;
  }

  public Object getMetric() {
    return metric;
  }

}
