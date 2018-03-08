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

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.eventbus.impl.EventBusImpl;
import io.vertx.core.eventbus.impl.HandlerHolder;
import io.vertx.core.eventbus.impl.MessageImpl;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.impl.HAManager;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.JksOptions;
import io.vertx.core.net.KeyCertOptions;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.core.net.PfxOptions;
import io.vertx.core.net.TCPSSLOptions;
import io.vertx.core.net.TrustOptions;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.core.spi.cluster.AsyncMultiMap;
import io.vertx.core.spi.cluster.ChoosableIterable;
import io.vertx.core.spi.cluster.ClusterManager;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

/**
 * An event bus implementation that clusters with other Vert.x nodes
 *
 * @author <a href="http://tfox.org">Tim Fox</a>   7                                                                                     T
 */
public class ClusteredEventBus extends EventBusImpl {

  private static final Logger log = LoggerFactory.getLogger(ClusteredEventBus.class);

  public static final String CLUSTER_PUBLIC_HOST_PROP_NAME = "vertx.cluster.public.host";
  public static final String CLUSTER_PUBLIC_PORT_PROP_NAME = "vertx.cluster.public.port";

  private static final Buffer PONG = Buffer.buffer(new byte[]{(byte) 1});
  private static final String SERVER_ID_HA_KEY = "server_id";
  private static final String SUBS_MAP_NAME = "__vertx.subs";

  private final ClusterManager clusterManager;
  private final HAManager haManager;
  private final ConcurrentMap<ServerID, ConnectionHolder> connections = new ConcurrentHashMap<>();
  private final Context sendNoContext;

  private EventBusOptions options;
  private AsyncMultiMap<String, ClusterNodeInfo> subs;
  private Set<String> ownSubs = new ConcurrentHashSet<>();
  private ServerID serverID;
  private ClusterNodeInfo nodeInfo;
  private NetServer server;

  public ClusteredEventBus(VertxInternal vertx,
                           VertxOptions options,
                           ClusterManager clusterManager,
                           HAManager haManager) {
    super(vertx);
    this.options = options.getEventBusOptions();
    this.clusterManager = clusterManager;
    this.haManager = haManager;
    this.sendNoContext = vertx.getOrCreateContext();
    setClusterViewChangedHandler(haManager);
  }

  private NetServerOptions getServerOptions() {
    NetServerOptions serverOptions = new NetServerOptions(this.options.toJson());
    setCertOptions(serverOptions, options.getKeyCertOptions());
    setTrustOptions(serverOptions, options.getTrustOptions());

    return serverOptions;
  }

  static void setCertOptions(TCPSSLOptions options, KeyCertOptions keyCertOptions) {
    if (keyCertOptions == null) {
      return;
    }
    if (keyCertOptions instanceof JksOptions) {
      options.setKeyStoreOptions((JksOptions) keyCertOptions);
    } else if (keyCertOptions instanceof PfxOptions) {
      options.setPfxKeyCertOptions((PfxOptions) keyCertOptions);
    } else {
      options.setPemKeyCertOptions((PemKeyCertOptions) keyCertOptions);
    }
  }

  static void setTrustOptions(TCPSSLOptions sslOptions, TrustOptions options) {
    if (options == null) {
      return;
    }

    if (options instanceof JksOptions) {
      sslOptions.setTrustStoreOptions((JksOptions) options);
    } else if (options instanceof PfxOptions) {
      sslOptions.setPfxTrustOptions((PfxOptions) options);
    } else {
      sslOptions.setPemTrustOptions((PemTrustOptions) options);
    }
  }

  @Override
  public void start(Handler<AsyncResult<Void>> resultHandler) {
    clusterManager.<String, ClusterNodeInfo>getAsyncMultiMap(SUBS_MAP_NAME, ar2 -> {
      if (ar2.succeeded()) {
        subs = ar2.result();
        // ClusteredEventBus 在启动的时候，会创建一个 NetServer，用于接收 eventBus.send() 发的集群的消息
        // eventBus.send() 发送消息后，会从集群中 hazelcast 中获取目标机器的结点信息，然后创建一个 NetClient，把消息
        // 发送到该 NetServer 上
        server = vertx.createNetServer(getServerOptions());

        // getServerHandler()，这里就是消息处理器
        server.connectHandler(getServerHandler());
        server.listen(asyncResult -> {
          if (asyncResult.succeeded()) {
            // ClusteredEventBus 启动的时候，会获取当前机器的 host port
            int serverPort = getClusterPublicPort(options, server.actualPort());
            String serverHost = getClusterPublicHost(options);
            // 把获取到的 host port，封装成 ServerID
            serverID = new ServerID(serverPort, serverHost);
            // 创建 nodeInfo
            nodeInfo = new ClusterNodeInfo(clusterManager.getNodeID(), serverID);
            haManager.addDataToAHAInfo(SERVER_ID_HA_KEY, new JsonObject().put("host", serverID.host).put("port", serverID.port));
            if (resultHandler != null) {
              started = true;
              resultHandler.handle(Future.succeededFuture());
            }
          } else {
            if (resultHandler != null) {
              resultHandler.handle(Future.failedFuture(asyncResult.cause()));
            } else {
              log.error(asyncResult.cause());
            }
          }
        });
      } else {
        if (resultHandler != null) {
          resultHandler.handle(Future.failedFuture(ar2.cause()));
        } else {
          log.error(ar2.cause());
        }
      }
    });
  }

  @Override
  public void close(Handler<AsyncResult<Void>> completionHandler) {
    super.close(ar1 -> {
      if (server != null) {
        server.close(ar -> {
          if (ar.failed()) {
            log.error("Failed to close server", ar.cause());
          }
          // Close all outbound connections explicitly - don't rely on context hooks
          for (ConnectionHolder holder : connections.values()) {
            holder.close();
          }
          if (completionHandler != null) {
            completionHandler.handle(ar);
          }
        });
      } else {
        if (completionHandler != null) {
          completionHandler.handle(ar1);
        }
      }
    });

  }

  /**
   * 重写了 EventBusImpl 中的方法，用于在集群模式下的消息体（MessageImpl）创建。
   * @param send  标志位
   * @param address 目标地址
   * @param headers 设置的 headers
   * @param body  发送的对象
   * @param codecName 消息编码解码器
   * @return
   */
  @Override
  protected MessageImpl createMessage(boolean send, String address, MultiMap headers, Object body, String codecName) {
    Objects.requireNonNull(address, "no null address accepted");
    MessageCodec codec = codecManager.lookupCodec(body, codecName);
    @SuppressWarnings("unchecked")
    ClusteredMessage msg = new ClusteredMessage(serverID, address, null, headers, body, codec, send, this);
    return msg;
  }

  /**
   * 重写 EventBusImpl 中的 addRegistration 方法
   *
   *
   */
  @Override
  protected <T> void addRegistration(boolean newAddress, String address,
                                     boolean replyHandler, boolean localOnly,
                                     Handler<AsyncResult<Void>> completionHandler) {

    // 判断
    // newAddress = true 代表对应的地址之前在本地中没有注册过，是刚注册的
    // subs != null
    // replyHandler 不是EventBus自动生成的
    // !localOnly 表示允许在集群范围内传播
    if (newAddress && subs != null && !replyHandler && !localOnly) {
      // Propagate the information
      // 将当前机器的位置添加到集群内的记录 subs 中
      // subs 是一个 AsyncMultiMap，key = address， value 是此 address 的机器位置的集合，因为 AsyncMultiMap 是一个 一键多值的 map，
      // 所以可以是一个 address 对应多个 nodeInfo（机器位置信息）
      subs.add(address, nodeInfo, completionHandler);
      ownSubs.add(address);
    } else {
      completionHandler.handle(Future.succeededFuture());
    }
  }

  /**
   * 重写 EventBusImpl 中的 removeRegistration 用于注销在集群模式下的注册信息
   */
  @Override
  protected <T> void removeRegistration(HandlerHolder lastHolder, String address,
                                        Handler<AsyncResult<Void>> completionHandler) {
    if (lastHolder != null && subs != null && !lastHolder.isLocalOnly()) {
      // ownSubs 中删除 address
      ownSubs.remove(address);
      // 调用 removeSub
      removeSub(address, nodeInfo, completionHandler);
    } else {
      callCompletionHandlerAsync(completionHandler);
    }
  }

  @Override
  protected <T> void sendReply(SendContextImpl<T> sendContext, MessageImpl replierMessage) {
    clusteredSendReply(((ClusteredMessage) replierMessage).getSender(), sendContext);
  }

  /**
   * 集群模式下，会重写 EventBusImpl 中的 sendOrPub 方法，执行自己的消息分发逻辑
   */
  @Override
  protected <T> void sendOrPub(SendContextImpl<T> sendContext) {
    // 从传入的 sendContext.message 中获取地址
    String address = sendContext.message.address();

    // 创建一个 handler，该handler 用于在 subs.get(address, handler) 时候使用
    // subs.get(address, handler) 会从 subs 中通过 address 查看，返回一个 AsyncResult<ChoosableIterable<ClusterNodeInfo>>> 结果
    // 然后通过 handler 处理
    Handler<AsyncResult<ChoosableIterable<ClusterNodeInfo>>> resultHandler = asyncResult -> {
      // 这里是 handler 处理
      if (asyncResult.succeeded()) {
        // 如果获取结果成功，把结果记录在 serverIDs 临时变量中
        ChoosableIterable<ClusterNodeInfo> serverIDs = asyncResult.result();
        // 判断 serverIDs 是否为空
        if (serverIDs != null && !serverIDs.isEmpty()) {
          // 如果不为空，则发送到远程服务器上
          sendToSubs(serverIDs, sendContext);
        } else {
          if (metrics != null) {
            metrics.messageSent(address, !sendContext.message.isSend(), true, false);
          }
          // 否则就在本地分发
          deliverMessageLocally(sendContext);
        }
      } else {
        log.error("Failed to send message", asyncResult.cause());
      }
    };
    if (Vertx.currentContext() == null) {
      // Guarantees the order when there is no current context
      sendNoContext.runOnContext(v -> {
        subs.get(address, resultHandler);
      });
    } else {
      subs.get(address, resultHandler);
    }
  }

  @Override
  protected String generateReplyAddress() {
    // The address is a cryptographically secure id that can't be guessed
    return UUID.randomUUID().toString();
  }

  @Override
  protected boolean isMessageLocal(MessageImpl msg) {
    ClusteredMessage clusteredMessage = (ClusteredMessage) msg;
    return !clusteredMessage.isFromWire();
  }

  private void setClusterViewChangedHandler(HAManager haManager) {
    haManager.setClusterViewChangedHandler(members -> {
      ownSubs.forEach(address -> {
        subs.add(address, nodeInfo, addResult -> {
          if (addResult.failed()) {
            log.warn("Failed to update subs map with self", addResult.cause());
          }
        });
      });

      subs.removeAllMatching((Serializable & Predicate<ClusterNodeInfo>) ci -> !members.contains(ci.nodeId), removeResult -> {
        if (removeResult.failed()) {
          log.warn("Error removing subs", removeResult.cause());
        }
      });
    });
  }

  private int getClusterPublicPort(EventBusOptions options, int actualPort) {
    // We retain the old system property for backwards compat
    int publicPort = Integer.getInteger(CLUSTER_PUBLIC_PORT_PROP_NAME, options.getClusterPublicPort());
    if (publicPort == -1) {
      // Get the actual port, wildcard port of zero might have been specified
      publicPort = actualPort;
    }
    return publicPort;
  }

  private String getClusterPublicHost(EventBusOptions options) {
    // We retain the old system property for backwards compat
    String publicHost = System.getProperty(CLUSTER_PUBLIC_HOST_PROP_NAME, options.getClusterPublicHost());
    if (publicHost == null) {
      publicHost = options.getHost();
    }
    return publicHost;
  }

  private Handler<NetSocket> getServerHandler() {
    return socket -> {
      // 这里为什么要 newFixed(4) ？
      // 答案可以参考 ClusteredMessage.encodeToWire()方法，后面的代码有 setBytes(0, buffer.length-4) 类似这样的代码，实际上是在设置 body 的长度
      // wire protocol 规定第一个 int 值为要发送的 Buffer 的长度，而一个 int = 4 个字节
      RecordParser parser = RecordParser.newFixed(4);
      Handler<Buffer> handler = new Handler<Buffer>() {
        int size = -1;

        public void handle(Buffer buff) {
          // 首先通过 getInt(0)，得到 body 的长度记录为 size
          // 并修正 parser
          if (size == -1) {
            size = buff.getInt(0);
            parser.fixedSizeMode(size);
          } else {
            // 通过 readFromWire 把 buffer 转换成 ClusteredMessage
            ClusteredMessage received = new ClusteredMessage();
            received.readFromWire(buff, codecManager);
            if (metrics != null) {
              metrics.messageRead(received.address(), buff.length());
            }
            // 然后再次修正，准备下一个流的读取
            parser.fixedSizeMode(4);
            // 设置 size = -1，可以让parser 下次继续先读取下一个流的 body 长度，然后继续转换 buffer -> ClusteredMessage 来处理，如此反复
            size = -1;
            // 如果接受到的消息的 codec 是 PING_MESSAGE_CODEC，则直接回复 PONG 消息
            if (received.codec() == CodecManager.PING_MESSAGE_CODEC) {
              // Just send back pong directly on connection
              socket.write(PONG);
            } else {
              // 通过 NetServer 获取到消息体后，还是通过本地的 deliverMessageLocally() 方法把
              // 消息体发送到本地的 handler 上处理。
              deliverMessageLocally(received);
            }
          }
        }
      };
      // 设置 parser 的output 处理器
      parser.setOutput(handler);
      // 设置 socket 的处理器 Handler，因为 parser 继承 Handler接口，又实现了 handle 方法，所以这里
      // 直接设置 parser 处理器，而 parser 实现的 handle 方法就是上面的那一串逻辑
      socket.handler(parser);
    };
  }

  /**
   * 这里是 集群模式下 EventBus.send() 发送消息方法
   * 这里分了 send publish 的不同逻辑
   */
  private <T> void sendToSubs(ChoosableIterable<ClusterNodeInfo> subs, SendContextImpl<T> sendContext) {
    String address = sendContext.message.address();
    if (sendContext.message.isSend()) {   // 如果是 点对点
      // Choose one
      ClusterNodeInfo ci = subs.choose();
      ServerID sid = ci == null ? null : ci.serverID;
      if (sid != null && !sid.equals(serverID)) {  //We don't send to this node
        // 如果 ServerID 不为空，并且不是本机的 ServerID
        if (metrics != null) {
          metrics.messageSent(address, false, false, true);
        }
        // 调用 sendRemote 方法，进行
        sendRemote(sid, sendContext.message);
      } else {
        // 如果 ServerID 不为空，并且是本机的 ServerID
        // 则调用 deliverMessageLocally
        if (metrics != null) {
          metrics.messageSent(address, false, true, false);
        }
        deliverMessageLocally(sendContext);
      }
    } else {    // 如果不是点对点，则用publish
      // Publish
      boolean local = false;
      boolean remote = false;
      for (ClusterNodeInfo ci : subs) {
        if (!ci.serverID.equals(serverID)) {  //We don't send to this node
          remote = true;
          sendRemote(ci.serverID, sendContext.message);
        } else {
          local = true;
        }
      }
      if (metrics != null) {
        metrics.messageSent(address, true, local, remote);
      }
      if (local) {
        deliverMessageLocally(sendContext);
      }
    }
  }

  private <T> void clusteredSendReply(ServerID replyDest, SendContextImpl<T> sendContext) {
    MessageImpl message = sendContext.message;
    String address = message.address();
    if (!replyDest.equals(serverID)) {
      if (metrics != null) {
        metrics.messageSent(address, false, false, true);
      }
      sendRemote(replyDest, message);
    } else {
      if (metrics != null) {
        metrics.messageSent(address, false, true, false);
      }
      deliverMessageLocally(sendContext);
    }
  }

  // 集群模式下，发送消息到远程服务器结点上
  private void sendRemote(ServerID theServerID, MessageImpl message) {
    // We need to deal with the fact that connecting can take some time and is async, and we cannot
    // block to wait for it. So we add any sends to a pending list if not connected yet.
    // Once we connect we send them.
    // This can also be invoked concurrently from different threads, so it gets a little
    // tricky
    ConnectionHolder holder = connections.get(theServerID);
    if (holder == null) {
      // When process is creating a lot of connections this can take some time
      // so increase the timeout
      // 创建一个 ConnectionHolder 内部实例化一个 NetClientImpl 的实例，并用 ConnectionHolder 封装
      holder = new ConnectionHolder(this, theServerID, options);
      // 把 ConnectionHolder 放入 connections 中
      // connections 是一个ConcurrentMap<ServerID, ConnectionHolder>
      // 可以观察下 ServerID 的 equals() 和 hashCode() 方法
      // putIfAbsent() 方法：不存在就把 value 放进去，并返回。存在就直接返回之前的（prev） = retrun map.get(key)
      ConnectionHolder prevHolder = connections.putIfAbsent(theServerID, holder);
      if (prevHolder != null) {
        // 如果 prevHolder != null，代表之前存在该 key，则直接返回该 value
        // Another one sneaked in
        holder = prevHolder;
      } else {
        // 否则把新创建的 ConnectionHolder 通过 connect() 方法建立连接
        // 并且检查了 pending 中的消息，遍历并发送出去
        holder.connect();
      }
    }
    // 上面的 holder.connect() 是一个同步方法
    // 这里调用 writeMessage 发送消息的时候可能客户端连接还没建立好，因此需要有一个缓冲区存放消息，等到建立好连接后再从缓冲区中遍历消息（之前放入的消息）发出去
    holder.writeMessage((ClusteredMessage) message);
  }

  /**
   * 把成员变量 subs 中删除指定的 key + node 的值
   */
  private void removeSub(String subName, ClusterNodeInfo node, Handler<AsyncResult<Void>> completionHandler) {
    subs.remove(subName, node, ar -> {
      if (!ar.succeeded()) {
        log.error("Failed to remove sub", ar.cause());
      } else {
        if (ar.result()) {
          if (completionHandler != null) {
            completionHandler.handle(Future.succeededFuture());
          }
        } else {
          if (completionHandler != null) {
            completionHandler.handle(Future.failedFuture("sub not found"));
          }
        }

      }
    });
  }

  ConcurrentMap<ServerID, ConnectionHolder> connections() {
    return connections;
  }

  VertxInternal vertx() {
    return vertx;
  }

  EventBusOptions options() {
    return options;
  }
}

