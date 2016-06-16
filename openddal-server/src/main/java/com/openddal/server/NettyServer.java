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

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.engine.Engine;
import com.openddal.engine.SysProperties;
import com.openddal.server.processor.ProcessorFactory;
import com.openddal.util.StringUtils;
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

    private static Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    /**
     * The default port to use for the server.
     */
    public static final int DEFAULT_LISTEN_PORT = 6100;

    private ServerArgs args;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor userExecutor;
    private ChannelFuture f;
    private Engine ddalEngine;

    public NettyServer(ServerArgs args) {
        this.args = args;
    }

    public Engine getDdalEngine() {
        return ddalEngine;
    }

    /**
     * Listen for incoming connections.
     */
    public void init() {

        try {
            if (!StringUtils.isNullOrEmpty(args.configFile)) {
                System.setProperty("ddal.engineConfigLocation", args.configFile);
            }
            LOGGER.info("{} server init ddal-engine from {}", getServerName(), SysProperties.ENGINE_CONFIG_LOCATION);
            Properties prop = new Properties();
            prop.setProperty(Engine.ENGINE_CONFIG_PROPERTY_NAME, SysProperties.ENGINE_CONFIG_LOCATION);
            ddalEngine = Engine.getImplicitEngine(prop);
            LOGGER.info("{} server ddal-engine inited.", getServerName());
        } catch (Exception e) {
            LOGGER.error("Exception happen when init ddal-engine ", e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    /**
     * Listen for incoming connections.
     */
    public void listen() {
        LOGGER.info("{} server is starting", getServerName());
        args.validate();
        ServerBootstrap b = configServer();
        try {
            // start server
            f = b.bind(args.port).sync();
            LOGGER.info("{} server started and listening on {}", getServerName(), args.port);
            // register shutown hook
            Runtime.getRuntime().addShutdownHook(new ShutdownThread());
        } catch (Exception e) {
            LOGGER.error("Exception happen when start " + getServerName() + " server", e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
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
        LOGGER.info("{} server is stopping", getServerName());
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
        int timeoutMills = args.shutdownTimeoutMills;
        Threads.shutdownGracefully(userExecutor, timeoutMills, timeoutMills, TimeUnit.SECONDS);
        LOGGER.info("{} server stoped", getServerName());
    }

    private ServerBootstrap configServer() {
        bossGroup = new NioEventLoopGroup(args.bossThreads, new DefaultThreadFactory("NettyBossGroup", true));
        workerGroup = new NioEventLoopGroup(args.workerThreads, new DefaultThreadFactory("NettyWorkerGroup", true));
        userExecutor = createUserThreadExecutor();
        Authenticator authenticator = createAuthenticator();
        ProcessorFactory processorFactory = createProcessorFactory();
        RequestFactory requestFactory = createRequestFactory();
        ResponseFactory responseFactory = createResponseFactory();
        final ProtocolHandler protocolHandler = new ProtocolHandler(authenticator, processorFactory, requestFactory,
                responseFactory, userExecutor);

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
                ch.pipeline().addLast(createProtocolDecoder(),
                        /* createProtocolEncoder(), */ protocolHandler);
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

    protected abstract String getServerName();

    protected abstract ChannelHandler createProtocolDecoder();

    protected abstract ChannelHandler createProtocolEncoder();

    protected abstract Authenticator createAuthenticator();

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
