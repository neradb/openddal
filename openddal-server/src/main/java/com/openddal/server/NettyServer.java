/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.util.Threads;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public class NettyServer {

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /**
     * The default port to use for the server.
     */
    public static final int DEFAULT_PORT = 6100;

    private ServerArgs args;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ExecutorService userThreadPool;
    private ChannelFuture f;

    public NettyServer(ServerArgs args) {
        this.args = args;
    }

    /**
     * Listen for incoming connections.
     */
    public void listen() {
        logger.info("Server server is starting");
        ServerBootstrap b = configServer();
        try {
            // start server
            int port = args.port > 0 ? args.port : DEFAULT_PORT;
            f = b.bind(port).sync();
            logger.info("Server started and listening on " + args.port);
            // register shutown hook
            Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        } catch (Exception e) {
            logger.error("Exception happen when start server", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * blocking to wait for close.
     */
    public void waitForClose() throws InterruptedException {
        f.channel().closeFuture().sync();
    }

    public void stop() {
        logger.info("Server is stopping");
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        Threads.shutdownGracefully(userThreadPool, args.shutdownTimeoutMills, args.shutdownTimeoutMills, TimeUnit.SECONDS);
        logger.info("Server stoped");
    }

    private ServerBootstrap configServer() {
        bossGroup = new NioEventLoopGroup(args.bossThreads, new DefaultThreadFactory("NettyBossGroup", true));
        workerGroup = new NioEventLoopGroup(args.workerThreads, new DefaultThreadFactory("NettyWorkerGroup", true));
        userThreadPool = Executors.newFixedThreadPool(args.userThreads, new DefaultThreadFactory("UserThreads", true));

        final ProtocolHandler protocolHandler = new ProtocolHandler(userThreadPool);

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_REUSEADDR, true).childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        if (args.socketTimeoutMills > 0) {
            b.childOption(ChannelOption.SO_TIMEOUT, args.socketTimeoutMills);
        }

        if (args.recvBuff > 0) {
            b.childOption(ChannelOption.SO_RCVBUF, args.recvBuff);
        }

        if (args.sendBuff > 0) {
            b.childOption(ChannelOption.SO_SNDBUF, args.sendBuff);
        }

        b.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ch.pipeline().addLast(createProtocolDecoder(), createTProtocolEncoder(), protocolHandler);
            }
        });

        return b;
    }

    private ChannelHandler createProtocolDecoder() {
        return new ProtocolDecoder();
    }

    private ChannelHandler createTProtocolEncoder() {
        return new ProtocolEncoder();
    }

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            NettyServer.this.stop();
        }
    }
}
