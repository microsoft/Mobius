/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.api.csharp

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue, TimeUnit}

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelInitializer, EventLoopGroup, ChannelFuture}
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}


/**
 * Netty server that invokes JVM calls based upon receiving
 * messages from C# in SparkCLR.
 * This implementation is identical to RBackend and that can be reused
 * in SparkCLR if the handler is made pluggable
 */
// Since SparkCLR is a package to Spark and not a part of spark-core it mirrors the implementation of
// selected parts from RBackend with SparkCLR customizations
class CSharpBackend { self => // for accessing the this reference in inner class(ChannelInitializer)
  private[this] var channelFuture: ChannelFuture = null
  private[this] var bootstrap: ServerBootstrap = null
  private[this] var bossGroup: EventLoopGroup = null

  def init(): Int = {
    // need at least 3 threads, use 10 here for safety
    bossGroup = new NioEventLoopGroup(10)
    val workerGroup = bossGroup

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])

    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast("encoder", new ByteArrayEncoder())
          .addLast("frameDecoder",
            // maxFrameLength = 2G
            // lengthFieldOffset = 0
            // lengthFieldLength = 4
            // lengthAdjustment = 0
            // initialBytesToStrip = 4, i.e. strip out the length field itself
            //new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addLast("decoder", new ByteArrayDecoder())
          //TODO - work with SparkR devs to make this configurable and reuse RBackend
          .addLast("handler", new CSharpBackendHandler(self))
      }
    })

    channelFuture = bootstrap.bind(new InetSocketAddress("localhost", 0))
    channelFuture.syncUninterruptibly()
    channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
  }

  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close(): Unit = {
    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully()
    }
    bootstrap = null

    // Send close to CSharp callback server.
    println("Requesting to close all call back sockets.")
    var socket: Socket = null
    do {
      socket = CSharpBackend.callbackSockets.poll()
      if (socket != null) {
        try {
          val dos = new DataOutputStream(socket.getOutputStream)
          SerDe.writeString(dos, "close")
          socket.close()
          socket = null
        }
      }
    } while (socket != null)
    CSharpBackend.callbackSocketShutdown = true
  }
}

object CSharpBackend {
  // Channels to callback server.
  private[spark] val callbackSockets: BlockingQueue[Socket] = new LinkedBlockingQueue[Socket]()
  @volatile private[spark] var callbackPort: Int = 0

  // flag to denote whether the callback socket is shutdown explicitly
  @volatile private[spark] var callbackSocketShutdown: Boolean = false
}
