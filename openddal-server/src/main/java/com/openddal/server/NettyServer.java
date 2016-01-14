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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.RequestFactory;
import com.openddal.server.processor.ResponseFactory;
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

public abstract class NettyServer {

    public static final byte PROTOCOL_VERSION = 10;

    public static final byte[] SERVER_VERSION = "openddal-server-1.0.0-SNAPSHOT".getBytes();

    private static Logger logger = LoggerFactory.getLogger(NettyServer.class);

    /**
     * The default port to use for the server.
     */
    public static final int DEFAULT_LISTEN_PORT = 6100;

    private ServerArgs args;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor userExecutor;
    private ChannelFuture f;

    public NettyServer(ServerArgs args) {
        this.args = args;
    }

    /**
     * Listen for incoming connections.
     */
    public void listen() {
        logger.info("Server server is starting");
        args.validate();
        ServerBootstrap b = configServer();
        try {
            // start server
            f = b.bind(args.port).sync();
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
        int timeoutMills = args.shutdownTimeoutMills;
        Threads.shutdownGracefully(userExecutor, timeoutMills, timeoutMills,
                TimeUnit.SECONDS);
        logger.info("Server stoped");
    }

    private ServerBootstrap configServer() {
        bossGroup = new NioEventLoopGroup(args.bossThreads, new DefaultThreadFactory("NettyBossGroup", true));
        workerGroup = new NioEventLoopGroup(args.workerThreads, new DefaultThreadFactory("NettyWorkerGroup", true));
        userExecutor = createUserThreadExecutor();
        ProcessorFactory processorFactory = createProcessorFactory();
        RequestFactory requestFactory = createRequestFactory();
        ResponseFactory responseFactory = createResponseFactory();
        final ProtocolHandler protocolHandler = new ProtocolHandler(processorFactory, requestFactory, responseFactory,
                userExecutor);

        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                .childOption(ChannelOption.SO_REUSEADDR, true).childOption(ChannelOption.SO_KEEPALIVE, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.TCP_NODELAY, true);

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
                ch.pipeline().addLast(createProtocolDecoder(), createProtocolEncoder(), protocolHandler);
            }
        });

        return b;
    }

    public ThreadPoolExecutor createUserThreadExecutor() {
        int corePoolSize = Runtime.getRuntime().availableProcessors();
        int capacity = 200;
        int maximumPoolSize = args.maxThreads;
        int keepAliveTime = args.keepAliveTime;
        BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(capacity);
        ThreadPoolExecutor userExecutor = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime,
                TimeUnit.MILLISECONDS, workQueue, new DefaultThreadFactory("request-processor", true),
                new AbortPolicy());
        userExecutor.allowCoreThreadTimeOut(true);
        return userExecutor;
    }

    protected abstract ChannelHandler createProtocolDecoder();

    protected abstract ChannelHandler createProtocolEncoder();

    protected abstract ProcessorFactory createProcessorFactory();

    protected abstract RequestFactory createRequestFactory();

    protected abstract ResponseFactory createResponseFactory();

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            NettyServer.this.stop();
        }
    }
}
