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

import io.vertx.core.AsyncResult;
import io.vertx.core.Closeable;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.core.eventbus.SendContext;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.spi.metrics.EventBusMetrics;
import io.vertx.core.spi.metrics.MetricsProvider;
import io.vertx.core.spi.metrics.VertxMetrics;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A local event bus implementation
 *
 * @author <a href="http://tfox.org">Tim Fox</a>                                                                                        T
 */
public class EventBusImpl implements EventBus, MetricsProvider {

  private static final Logger log = LoggerFactory.getLogger(EventBusImpl.class);

  private final List<Handler<SendContext>> interceptors = new CopyOnWriteArrayList<>();
  private final AtomicLong replySequence = new AtomicLong(0);
  protected final VertxInternal vertx;
  protected final EventBusMetrics metrics;
  protected final ConcurrentMap<String, Handlers> handlerMap = new ConcurrentHashMap<>();
  protected final CodecManager codecManager = new CodecManager();
  protected volatile boolean started;

  public EventBusImpl(VertxInternal vertx) {
    VertxMetrics metrics = vertx.metricsSPI();
    this.vertx = vertx;
    this.metrics = metrics != null ? metrics.createMetrics(this) : null;
  }

  @Override
  public EventBus addInterceptor(Handler<SendContext> interceptor) {
    interceptors.add(interceptor);
    return this;
  }

  @Override
  public EventBus removeInterceptor(Handler<SendContext> interceptor) {
    interceptors.remove(interceptor);
    return this;
  }

  // local 模式的 EventBus 启动方法，在 VertxImpl 中调用
  // 为了防止 race condition ，方法是 synchronized
  public synchronized void start(Handler<AsyncResult<Void>> completionHandler) {
    // started 成员变量是 volatile，这样可以保证其可见性，确保其它线程通过 checkStarted 方法读取到的 started 结果总是新的
    if (started) {
      throw new IllegalStateException("Already started");
    }
    started = true;
    completionHandler.handle(Future.succeededFuture());
  }

  @Override
  public EventBus send(String address, Object message) {
    return send(address, message, new DeliveryOptions(), null);
  }

  @Override
  public <T> EventBus send(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
    return send(address, message, new DeliveryOptions(), replyHandler);
  }

  @Override
  public EventBus send(String address, Object message, DeliveryOptions options) {
    return send(address, message, options, null);
  }

  @Override
  public <T> EventBus send(String address, Object message, DeliveryOptions options, Handler<AsyncResult<Message<T>>> replyHandler) {
    // send & publish 最终都会调用该方法 sendOrPubInternal
    sendOrPubInternal(createMessage(true, address, options.getHeaders(), message, options.getCodecName()), options, replyHandler);
    return this;
  }

  @Override
  public <T> MessageProducer<T> sender(String address) {
    Objects.requireNonNull(address, "address");
    return new MessageProducerImpl<>(vertx, address, true, new DeliveryOptions());
  }

  @Override
  public <T> MessageProducer<T> sender(String address, DeliveryOptions options) {
    Objects.requireNonNull(address, "address");
    Objects.requireNonNull(options, "options");
    return new MessageProducerImpl<>(vertx, address, true, options);
  }

  @Override
  public <T> MessageProducer<T> publisher(String address) {
    Objects.requireNonNull(address, "address");
    return new MessageProducerImpl<>(vertx, address, false, new DeliveryOptions());
  }

  @Override
  public <T> MessageProducer<T> publisher(String address, DeliveryOptions options) {
    Objects.requireNonNull(address, "address");
    Objects.requireNonNull(options, "options");
    return new MessageProducerImpl<>(vertx, address, false, options);
  }

  @Override
  public EventBus publish(String address, Object message) {
    return publish(address, message, new DeliveryOptions());
  }

  @Override
  public EventBus publish(String address, Object message, DeliveryOptions options) {
    sendOrPubInternal(createMessage(false, address, options.getHeaders(), message, options.getCodecName()), options, null);
    return this;
  }

  @Override
  public <T> MessageConsumer<T> consumer(String address) {
    // 检查是否已经启动
    checkStarted();
    // address 不能为空
    Objects.requireNonNull(address, "address");
    // 创建并返回 MessageConsumer
    // 这里的 public class HandlerRegistration<T> implements MessageConsumer<T>, Handler<Message<T>>
    // HandlerRegistration 相当于一个 "handler注册记录"
    return new HandlerRegistration<>(vertx, metrics, this, address, null, false, null, -1);
  }

  /**
   * 1. 检查 handler 是否为空
   * 2. 调用 consumer(String address) 方法创建 MessageConsumer
   * 3. 在 MessageConsumer 中设置 handler
   * 4. 返回 MessageConsumer
   *
   * @param address  the address that will register it at
   * @param handler  the handler that will process the received messages
   */
  @Override
  public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
    Objects.requireNonNull(handler, "handler");
    MessageConsumer<T> consumer = consumer(address);
    // 调用 MessageConsumer 的 handler 方法绑定对应的消息处理函数
    consumer.handler(handler);
    return consumer;
  }

  @Override
  public <T> MessageConsumer<T> localConsumer(String address) {
    checkStarted();
    Objects.requireNonNull(address, "address");
    return new HandlerRegistration<>(vertx, metrics, this, address, null, true, null, -1);
  }

  @Override
  public <T> MessageConsumer<T> localConsumer(String address, Handler<Message<T>> handler) {
    Objects.requireNonNull(handler, "handler");
    MessageConsumer<T> consumer = localConsumer(address);
    consumer.handler(handler);
    return consumer;
  }

  @Override
  public EventBus registerCodec(MessageCodec codec) {
    codecManager.registerCodec(codec);
    return this;
  }

  @Override
  public EventBus unregisterCodec(String name) {
    codecManager.unregisterCodec(name);
    return this;
  }

  @Override
  public <T> EventBus registerDefaultCodec(Class<T> clazz, MessageCodec<T, ?> codec) {
    codecManager.registerDefaultCodec(clazz, codec);
    return this;
  }

  @Override
  public EventBus unregisterDefaultCodec(Class clazz) {
    codecManager.unregisterDefaultCodec(clazz);
    return this;
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    checkStarted();
    unregisterAll();
    if (metrics != null) {
      metrics.close();
    }
    if (completionHandler != null) {
      vertx.runOnContext(v -> completionHandler.handle(Future.succeededFuture()));
    }
  }

  @Override
  public boolean isMetricsEnabled() {
    return metrics != null;
  }

  @Override
  public EventBusMetrics<?> getMetrics() {
    return metrics;
  }

  /**
   * 1. 首先确保 address 地址不为空
   * 2. 通过 codecManager 获取相应的 MessageCodec，如果没有提供 Codec（即 codecName 为空），那么会使用内置的查询，lookupCodec 方法
   * 3. 准备好 MessageCodec 后，创建一个 MessageImpl 实例，并返回，new MessageImpl(......)
   *
   * @param send  标志位
   * @param address 目标地址
   * @param headers 设置的 headers
   * @param body  发送的对象
   * @param codecName 消息编码解码器
   * @return MessageImpl
   *
   * 在集群模式下，该方法会被 ClusteredEventBus 重写，具体看下 ClusteredEventBus 中 createMessage 的方法
   */
  protected MessageImpl createMessage(boolean send, String address, MultiMap headers, Object body, String codecName) {
    Objects.requireNonNull(address, "no null address accepted");
    MessageCodec codec = codecManager.lookupCodec(body, codecName);
    @SuppressWarnings("unchecked")
    MessageImpl msg = new MessageImpl(address, null, headers, body, codec, send, this);
    return msg;
  }

  // 注册 consumer 到 EventBus 上
  protected <T> void addRegistration(String address, HandlerRegistration<T> registration,
                                     boolean replyHandler, boolean localOnly) {
    // 判断消息处理函数不能为空
    Objects.requireNonNull(registration.getHandler(), "handler");
    // 调用 addLocalRegistration 注册到本地
    boolean newAddress = addLocalRegistration(address, registration, replyHandler, localOnly);

    // 这里 Local 模式的 EventBus 用处不大，仅仅是调用 EventBusImpl.addRegistration(boolean newAddress, String address, boolean replyHandler, boolean localOnly, Handler<AsyncResult<Void>> completionHandler)方法
    // 即下面这个方法

    // 但是当使用 ClusteredEventBus.consumer 时候，就用处大了
    // ClusteredEventBus 中重写了 addRegistration() 方法
    addRegistration(newAddress, address, replyHandler, localOnly, registration::setResult);
  }

  protected <T> void addRegistration(boolean newAddress, String address,
                                     boolean replyHandler, boolean localOnly,
                                     Handler<AsyncResult<Void>> completionHandler) {
    completionHandler.handle(Future.succeededFuture());
  }

  protected <T> boolean addLocalRegistration(String address, HandlerRegistration<T> registration,
                                             boolean replyHandler, boolean localOnly) {
    Objects.requireNonNull(address, "address");

    // 获取当前线程对应的 Vert.x Context
    // currentContext 会检查是否是 VertxThread 的 context， 如果是，则返回。否则返回 null
    Context context = Vertx.currentContext();
    boolean hasContext = context != null;
    // 如果获取不到，说明当前不在 Verticle 中（即 Embedded），需要调用 vertx.getOrCreateContext 来获取 Context
    if (!hasContext) {
      // Embedded
      context = vertx.getOrCreateContext();
    }
    registration.setHandlerContext(context);

    boolean newAddress = false;

    // HandlerHolder 类包装了 registration 和 context
    HandlerHolder holder = new HandlerHolder<>(metrics, registration, replyHandler, localOnly, context);

    // 从哈希表中 handlerMap 获取给定地址的 Handlers
    // handlers 是一个 ConcurrentMap<String, Handlers> 哈希表， key = address， value = Handlers（该类代表一些 Handler 的集合，它内部维护着一个列表 list，用于存储每个HandlerHolder（HandlerHolder的集合）
    // Handlers 中有一个 choose 函数，次函数根据轮训算法从 HandlerHolder 集合中选定一个 HandlerHolder
    Handlers handlers = handlerMap.get(address);
    if (handlers == null) {
      // 如果 handlers 为空，则新建一个 handlers，内部是一个 CopyOnWriteList，维护者一个 HandlerHolder 集合
      // 如果 handlers 为空，代表该 address 上还没有注册消息处理函数（handler）
      handlers = new Handlers();
      Handlers prevHandlers = handlerMap.putIfAbsent(address, handlers);
      if (prevHandlers != null) {
        handlers = prevHandlers;
      }
      // newAddress = true，代表这是一个新注册的地址。有什么用？
      // 答案，返回值。并且在 ClusteredEventBus 中会判断是否是 newAddress
      newAddress = true;
    }
    // 添加一个 HandlerHolder 到该 handlers 中
    handlers.list.add(holder);

    // 设置 close hook 的作用是？
    // 如果当前线程在 Vert.x Context 中（比如 Verticle 中）
    // vert.x 会通过 addCloseHook 方法给当前的 context 添加一个钩子用于注销当前绑定的 registration。
    // 这样当对应的 verticle 被undeploy 的时候，此 verticle 绑定的所有消息处理handler都会被unregister。
    // Hook 的类型是 HandlerEntry，它继承了 Closeable 接口，对应的逻辑在 close 函数中
    if (hasContext) {
      HandlerEntry entry = new HandlerEntry<>(address, registration);
      context.addCloseHook(entry);
    }

    return newAddress;
  }

  // 注销
  protected <T> void removeRegistration(String address, HandlerRegistration<T> handler, Handler<AsyncResult<Void>> completionHandler) {
    // 注销本地的 address - handler
    HandlerHolder holder = removeLocalRegistration(address, handler);
    removeRegistration(holder, address, completionHandler);
  }

  // 该方法会被 ClusteredEventBus 重写，用于对集群模式下的注销
  protected <T> void removeRegistration(HandlerHolder handlerHolder, String address,
                                        Handler<AsyncResult<Void>> completionHandler) {
    callCompletionHandlerAsync(completionHandler);
  }

  protected <T> HandlerHolder removeLocalRegistration(String address, HandlerRegistration<T> handler) {
    Handlers handlers = handlerMap.get(address);
    HandlerHolder lastHolder = null;
    if (handlers != null) {
      synchronized (handlers) {
        int size = handlers.list.size();
        // Requires a list traversal. This is tricky to optimise since we can't use a set since
        // we need fast ordered traversal for the round robin
        for (int i = 0; i < size; i++) {
          HandlerHolder holder = handlers.list.get(i);
          if (holder.getHandler() == handler) {
            handlers.list.remove(i);
            holder.setRemoved();
            if (handlers.list.isEmpty()) {
              handlerMap.remove(address);
              lastHolder = holder;
            }
            holder.getContext().removeCloseHook(new HandlerEntry<>(address, holder.getHandler()));
            break;
          }
        }
      }
    }
    return lastHolder;
  }

  protected <T> void sendReply(MessageImpl replyMessage, MessageImpl replierMessage, DeliveryOptions options,
                               Handler<AsyncResult<Message<T>>> replyHandler) {
    if (replyMessage.address() == null) {
      throw new IllegalStateException("address not specified");
    } else {
      HandlerRegistration<T> replyHandlerRegistration = createReplyHandlerRegistration(replyMessage, options, replyHandler);
      new ReplySendContextImpl<>(replyMessage, options, replyHandlerRegistration, replierMessage).next();
    }
  }

  protected <T> void sendReply(SendContextImpl<T> sendContext, MessageImpl replierMessage) {
    sendOrPub(sendContext);
  }

  /**
   * 该方法仅仅在 metrics 模块中记录相关数据（messageSent），最后调用 deliverMessageLocally 方法执行发送消息的逻辑
   *
   * 集群模式：该方法在集群模式下，会被 ClusteredEventBus 重写，具体看下重写的逻辑，就可以知道集群模式下的 消息发送逻辑了
   */
  protected <T> void sendOrPub(SendContextImpl<T> sendContext) {
    MessageImpl message = sendContext.message;
    if (metrics != null) {
      metrics.messageSent(message.address(), !message.isSend(), true, false);
    }
    deliverMessageLocally(sendContext);
  }

  protected <T> Handler<Message<T>> convertHandler(Handler<AsyncResult<Message<T>>> handler) {
    return reply -> {
      Future<Message<T>> result;
      if (reply.body() instanceof ReplyException) {
        // This is kind of clunky - but hey-ho
        ReplyException exception = (ReplyException) reply.body();
        if (metrics != null) {
          metrics.replyFailure(reply.address(), exception.failureType());
        }
        result = Future.failedFuture(exception);
      } else {
        result = Future.succeededFuture(reply);
      }
      handler.handle(result);
    };
  }

  protected void callCompletionHandlerAsync(Handler<AsyncResult<Void>> completionHandler) {
    if (completionHandler != null) {
      vertx.runOnContext(v -> {
        completionHandler.handle(Future.succeededFuture());
      });
    }
  }

  protected <T> void deliverMessageLocally(SendContextImpl<T> sendContext) {
    if (!deliverMessageLocally(sendContext.message)) {
      // no handlers
      if (metrics != null) {
        metrics.replyFailure(sendContext.message.address, ReplyFailure.NO_HANDLERS);
      }
      if (sendContext.handlerRegistration != null) {
        sendContext.handlerRegistration.sendAsyncResultFailure(ReplyFailure.NO_HANDLERS, "No handlers for address "
                                                               + sendContext.message.address);
      }
    }
  }

  protected boolean isMessageLocal(MessageImpl msg) {
    return true;
  }

  /**
   * 真正发送消息的处理函数（这里才会区分 send publish 的发送逻辑，通过 msg.isSend() 标志位来区分）
   */
  protected <T> boolean deliverMessageLocally(MessageImpl msg) {
    msg.setBus(this);
    // 根据 address 获取 handlers 对象，handlers 中有一个 HandlerHolder 的 CopyOnWriteList，维护者所有注册在
    // 该地址上的 Handler
    Handlers handlers = handlerMap.get(msg.address());
    if (handlers != null) {
      if (msg.isSend()) {
        // 判断是否是 点对点，是 点对点，则 choose 一个，进行发送
        //Choose one
        HandlerHolder holder = handlers.choose();
        if (metrics != null) {
          metrics.messageReceived(msg.address(), !msg.isSend(), isMessageLocal(msg), holder != null ? 1 : 0);
        }
        if (holder != null) {
          // 如果找到了一个 Handler ，则调用 deliverToHandler 方法进行发送
          deliverToHandler(msg, holder);
        }
      } else {
        // 不是 点对点 发送，则 publish
        // Publish
        if (metrics != null) {
          metrics.messageReceived(msg.address(), !msg.isSend(), isMessageLocal(msg), handlers.list.size());
        }
        // 遍历发送
        for (HandlerHolder holder: handlers.list) {
          deliverToHandler(msg, holder);
        }
      }
      return true;
    } else {
      if (metrics != null) {
        metrics.messageReceived(msg.address(), !msg.isSend(), isMessageLocal(msg), 0);
      }
      return false;
    }
  }

  protected void checkStarted() {
    if (!started) {
      throw new IllegalStateException("Event Bus is not started");
    }
  }

  protected String generateReplyAddress() {
    return Long.toString(replySequence.incrementAndGet());
  }

  private <T> HandlerRegistration<T> createReplyHandlerRegistration(MessageImpl message,
                                                                    DeliveryOptions options,
                                                                    Handler<AsyncResult<Message<T>>> replyHandler) {
    if (replyHandler != null) {
      // 从配置中获取 reply 的最大超时时长，默认 30s
      long timeout = options.getSendTimeout();
      // 生成回复地址，回复地址 replyAddress 是（1、2、3、4 这样的自增数字）
      String replyAddress = generateReplyAddress();
      // 将 replyAddress 设置到 MessageImpl 中
      message.setReplyAddress(replyAddress);
      // 通过 convertHandler 方法对 replyHandler 进行封装，生成简化后的 Handler<Message<T>> 的 simpleReplyHandler
      Handler<Message<T>> simpleReplyHandler = convertHandler(replyHandler);
      // 创建一个 reply consumer，这个 consumer 是一次性的
      HandlerRegistration<T> registration =
        new HandlerRegistration<>(vertx, metrics, this, replyAddress, message.address, true, replyHandler, timeout);
      // 把 reply handler 设置到 reply consumer
      registration.handler(simpleReplyHandler);
      return registration;
    } else {
      return null;
    }
  }

  /**
   *
   * @param message 要发送的消息 MessageImpl
   * @param options 配置选项
   * @param replyHandler  回复处理函数
   */
  private <T> void sendOrPubInternal(MessageImpl message, DeliveryOptions options,
                                     Handler<AsyncResult<Message<T>>> replyHandler) {
    checkStarted();
    // 如果 replyHandler 不为空，则通过 createReplyHandlerRegistratio 会创建一个 reply consumer 实例
    // 如果 replyHandler 为空，则直接返回 null
    // 这个 reply consumer 是一次性的，也就是说 Vert.x 会在其接收到回复或超时的时候自动对其进行注销
    HandlerRegistration<T> replyHandlerRegistration = createReplyHandlerRegistration(message, options, replyHandler);

    // 创建一个 SendContextImpl（包装 MessageImpl、options、reply consumer）实例
    SendContextImpl<T> sendContext = new SendContextImpl<>(message, options, replyHandlerRegistration);

    // 调用 next() 方法
    // SendContext 有三个方法：message、next、send
    // message()：获取当前 SendContext 包装的消息实体（MessageImpl）
    // next()：调用下一个消息拦截器
    // send()：代表消息的发送模式是否为点对点模式
    sendContext.next();
  }

  protected class SendContextImpl<T> implements SendContext<T> {

    public final MessageImpl message;
    public final DeliveryOptions options;
    public final HandlerRegistration<T> handlerRegistration;
    public final Iterator<Handler<SendContext>> iter;

    public SendContextImpl(MessageImpl message, DeliveryOptions options, HandlerRegistration<T> handlerRegistration) {
      this.message = message;
      this.options = options;
      this.handlerRegistration = handlerRegistration;
      this.iter = interceptors.iterator();
    }

    @Override
    public Message<T> message() {
      return message;
    }


    /**
     * 消息拦截器本质上是一个 Handler<SendContext> 类型的处理函数
     * EventBusImpl 内部有一个成员变量 interceptors 存储着这些绑定的消息拦截器
     * 如果要进行链式拦截，则应该在每个拦截器中都调用对应的 SendContext.next() 方法，比如：
     *
     * eventBus.addInterceptor( sc -> {
     *  // 一些处理逻辑
     *  sc.next();  // 调用下一个拦截器
     * });
     *
     * 当所有拦截器都处理过后，会调用 sendOrPub(this); 方法进行消息的发送操作
     */
    @Override
    public void next() {
      if (iter.hasNext()) {
        Handler<SendContext> handler = iter.next();
        try {
          // handle 操作中，可以调用 SendContext.next() 方法，来继续迭代 iter.hasNext()
          // 寻找是否还有拦截器，如果有则继续执行拦截器处理函数，否则调用 sendOrPub 发送消息
          handler.handle(this);
        } catch (Throwable t) {
          log.error("Failure in interceptor", t);
        }
      } else {
        sendOrPub(this);
      }
    }

    @Override
    public boolean send() {
      return message.isSend();
    }

    @Override
    public Object sentBody() {
      return message.sentBody;
    }
  }

  protected class ReplySendContextImpl<T> extends SendContextImpl<T> {

    private final MessageImpl replierMessage;

    public ReplySendContextImpl(MessageImpl message, DeliveryOptions options, HandlerRegistration<T> handlerRegistration,
                                MessageImpl replierMessage) {
      super(message, options, handlerRegistration);
      this.replierMessage = replierMessage;
    }

    @Override
    public void next() {
      if (iter.hasNext()) {
        Handler<SendContext> handler = iter.next();
        handler.handle(this);
      } else {
        sendReply(this, replierMessage);
      }
    }
  }


  private void unregisterAll() {
    // Unregister all handlers explicitly - don't rely on context hooks
    for (Handlers handlers: handlerMap.values()) {
      for (HandlerHolder holder: handlers.list) {
        holder.getHandler().unregister();
      }
    }
  }

  /**
   * 发送消息到消息处理函数 handler
   */
  private <T> void deliverToHandler(MessageImpl msg, HandlerHolder<T> holder) {
    // Each handler gets a fresh copy
    // 复制一份要发送的消息
    @SuppressWarnings("unchecked")
    Message<T> copied = msg.copyBeforeReceive();

    if (metrics != null) {
      metrics.scheduleMessage(holder.getHandler().getMetric(), msg.isLocal());
    }

    // 通过handler.getContext() 获取之前传入的 Vert.x Context
    // 调用 runOnContext 方法以便让消息处理器在 Vert.x Context 中执行。为防止对应的处理器在处理之前被删除，还需要检查 holder
    // 的 isRemoved 属性
    holder.getContext().runOnContext((v) -> {
      // Need to check handler is still there - the handler might have been removed after the message were sent but
      // before it was received
      try {
        if (!holder.isRemoved()) {
          // 调用 holder中handler的handle方法，处理该消息
          // 注意这里获取到的实际上是一个 HandlerRegistration，但是 HandlerRegistration 同时实现了 MessageConsumer、Handler接口
          // 所以它兼具这两个接口的功能
          holder.getHandler().handle(copied);
        }
      } finally {
        // 最后，Vert.x 检查 holder 中的 handler 是否是 reply consumer（reply handler）
        // 如果是的话则调用其 unregister() 方法注销，确保 reply consumer 是一次性的
        if (holder.isReplyHandler()) {
          holder.getHandler().unregister();
        }
      }
    });
  }

  public class HandlerEntry<T> implements Closeable {
    final String address;
    final HandlerRegistration<T> handler;

    public HandlerEntry(String address, HandlerRegistration<T> handler) {
      this.address = address;
      this.handler = handler;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null) return false;
      if (this == o) return true;
      if (getClass() != o.getClass()) return false;
      HandlerEntry entry = (HandlerEntry) o;
      if (!address.equals(entry.address)) return false;
      if (!handler.equals(entry.handler)) return false;
      return true;
    }

    @Override
    public int hashCode() {
      int result = address != null ? address.hashCode() : 0;
      result = 31 * result + (handler != null ? handler.hashCode() : 0);
      return result;
    }

    // Called by context on undeploy
    public void close(Handler<AsyncResult<Void>> completionHandler) {
      handler.unregister(completionHandler);
    }

  }

  @Override
  protected void finalize() throws Throwable {
    // Make sure this gets cleaned up if there are no more references to it
    // so as not to leave connections and resources dangling until the system is shutdown
    // which could make the JVM run out of file handles.
    close(ar -> {});
    super.finalize();
  }

}

