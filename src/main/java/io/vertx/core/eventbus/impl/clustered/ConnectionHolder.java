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

package io.vertx.core.eventbus.impl.clustered;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.eventbus.impl.codecs.PingMessageCodec;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.impl.NetClientImpl;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.spi.metrics.EventBusMetrics;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
class ConnectionHolder {

  private static final Logger log = LoggerFactory.getLogger(ConnectionHolder.class);

  private static final String PING_ADDRESS = "__vertx_ping";

  private final ClusteredEventBus eventBus;
  private final NetClient client;
  private final ServerID serverID;
  private final Vertx vertx;
  private final EventBusMetrics metrics;

  private Queue<ClusteredMessage> pending;
  private NetSocket socket;
  private boolean connected;
  private long timeoutID = -1;
  private long pingTimeoutID = -1;

  ConnectionHolder(ClusteredEventBus eventBus, ServerID serverID, EventBusOptions options) {
    this.eventBus = eventBus;
    this.serverID = serverID;
    this.vertx = eventBus.vertx();
    this.metrics = eventBus.getMetrics();
    NetClientOptions clientOptions = new NetClientOptions(options.toJson());
    ClusteredEventBus.setCertOptions(clientOptions, options.getKeyCertOptions());
    ClusteredEventBus.setTrustOptions(clientOptions, options.getTrustOptions());
    client = new NetClientImpl(eventBus.vertx(), clientOptions, false);
  }

  synchronized void connect() {
    if (connected) {
      throw new IllegalStateException("Already connected");
    }
    // 这个 client 就是在 new ConnectionHolder 的构造器中创建的 NetClientImpl 实例
    // serverID 是发送目的服务器的结点信息对象
    client.connect(serverID.port, serverID.host, res -> {
      if (res.succeeded()) {
        // 连接成功后调用 connected 方法
        connected(res.result());
      } else {
        log.warn("Connecting to server " + serverID + " failed", res.cause());
        close();
      }
    });
  }

  // TODO optimise this (contention on monitor)
  synchronized void writeMessage(ClusteredMessage message) {
    if (connected) {
      // 这里把 ClusteredMessage 编码成 wire protocol 的 Buffer 对象（本质上是一个 Netty 的 Unpooled.buffer()，即堆内存）JVM 会检查当 GC ROOT 不可达的时候自动回收这部分内存
      // vert.x 中的 Buffer.buffer()，底层是调用的 Unpooled.unreleasableBuffer(Unpooled.buffer(...), ...); 所以本质上是一个 堆内存（没有池化），所以 JVM 会自动回收
      // 这里为什么要用 堆内存呢？ 因为发送消息是一次性的，每次发送的Buffer都可能不一样，所以池化没有必要，反而会让内存爆掉。
      // 使用 Unpooled.buffer 可以在一次消息发送完成后，等待 JVM GC 后自动被回收释放内存
      Buffer data = message.encodeToWire();
      if (metrics != null) {
        metrics.messageWritten(message.address(), data.length());
      }
      socket.write(data);
    } else {
      if (pending == null) {
        if (log.isDebugEnabled()) {
          log.debug("Not connected to server " + serverID + " - starting queuing");
        }
        pending = new ArrayDeque<>();
      }
      pending.add(message);
    }
  }

  void close() {
    if (timeoutID != -1) {
      vertx.cancelTimer(timeoutID);
    }
    if (pingTimeoutID != -1) {
      vertx.cancelTimer(pingTimeoutID);
    }
    try {
      client.close();
    } catch (Exception ignore) {
    }
    // The holder can be null or different if the target server is restarted with same serverid
    // before the cleanup for the previous one has been processed
    if (eventBus.connections().remove(serverID, this)) {
      if (log.isDebugEnabled()) {
        log.debug("Cluster connection closed for server " + serverID);
      }
    }
  }

  private void schedulePing() {
    EventBusOptions options = eventBus.options();
    pingTimeoutID = vertx.setTimer(options.getClusterPingInterval(), id1 -> {
      // If we don't get a pong back in time we close the connection
      timeoutID = vertx.setTimer(options.getClusterPingReplyInterval(), id2 -> {
        // Didn't get pong in time - consider connection dead
        log.warn("No pong from server " + serverID + " - will consider it dead");
        close();
      });
      ClusteredMessage pingMessage =
        new ClusteredMessage<>(serverID, PING_ADDRESS, null, null, null, new PingMessageCodec(), true, eventBus);
      Buffer data = pingMessage.encodeToWire();
      socket.write(data);
    });
  }

  private synchronized void connected(NetSocket socket) {
    this.socket = socket;
    connected = true;
    // 给 socket 设置异常处理、关闭处理
    socket.exceptionHandler(t -> close());
    socket.closeHandler(v -> close());

    // 处理消息
    socket.handler(data -> {
      // Got a pong back
      vertx.cancelTimer(timeoutID);
      schedulePing();
    });
    // Start a pinger
    schedulePing();
    if (pending != null) {
      if (log.isDebugEnabled()) {
        log.debug("Draining the queue for server " + serverID);
      }
      // 如果建立连接后发现缓冲区中有消息，则遍历并尝试发送消息
      for (ClusteredMessage message : pending) {
        // 把 ClusteredMessage 编码成 wire 协议体
        Buffer data = message.encodeToWire();
        if (metrics != null) {
          metrics.messageWritten(message.address(), data.length());
        }
        // 发送数据
        socket.write(data);
      }
    }
    pending = null;
  }

}
