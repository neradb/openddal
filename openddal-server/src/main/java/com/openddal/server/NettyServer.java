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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.engine.Engine;
import com.openddal.engine.SessionFactoryBuilder;
import com.openddal.engine.SysProperties;
import com.openddal.server.core.QueryDispatcher;
import com.openddal.server.core.ServerSession;
import com.openddal.server.mysql.auth.Privilege;
import com.openddal.server.mysql.auth.PrivilegeDefault;
import com.openddal.util.ExtendableThreadPoolExecutor;
import com.openddal.util.ExtendableThreadPoolExecutor.TaskQueue;
import com.openddal.util.New;
import com.openddal.util.Threads;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

public abstract class NettyServer {

    private static Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

    private final AtomicLong threadIdGenerator = new AtomicLong(300);
    /**
     * The default port to use for the server.
     */
    public static final int DEFAULT_LISTEN_PORT = 6100;

    private ServerArgs args;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ThreadPoolExecutor userExecutor;
    private ChannelFuture f;
    private Engine engine;
    private Privilege privilege = PrivilegeDefault.getPrivilege();
    private ConcurrentMap<Long, ServerSession> sessions = New.concurrentHashMap();
    private final Map<String, String> variables = New.hashMap();
    private long uptime;

    public NettyServer(ServerArgs args) {
        this.args = args;
    }

    public Engine getEngine() {
        return engine;
    }

    public ThreadPoolExecutor getUserExecutor() {
        return userExecutor;
    }

    public Privilege getPrivilege() {
        return privilege;
    }
    
    public long generateThreadId() {
        return threadIdGenerator.incrementAndGet();
    }

    public void registerSession(ServerSession session) {
        sessions.put(session.getThreadId(), session);
    }

    public void removeSession(long threadId) {
        sessions.remove(threadId);
    }

    public ServerSession getSession(long threadId) {
        return sessions.get(threadId);
    }

    /**
     * @return the sessions
     */
    public Collection<ServerSession> getSessions() {
        return sessions.values();
    }
    
    public Map<String, String> getStatus() {
        ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
        Map<String, String> status = New.hashMap();
        status.put("Uptime", String.valueOf(uptime));
        status.put("Uptime_since_flush_status", String.valueOf(System.currentTimeMillis()));
        status.put("Compression", "OFF");
        status.put("Connections", String.valueOf(sessions.size()));
        status.put("Threads_running", String.valueOf(threadBean.getThreadCount()));
        status.put("Threads_peak", String.valueOf(threadBean.getPeakThreadCount()));
        status.put("Threads_created", String.valueOf(threadBean.getTotalStartedThreadCount()));
        status.put("Threads_connected", String.valueOf(sessions.size()));
        status.put("User_threads_executor", getUserExecutor().toString());
        
        return status;
    }

    /**
     * @return the variables
     */
    public Map<String, String> getVariables() {
        return variables;
    }

    /**
     * Listen for incoming connections.
     */
    public void init() {
        try {
            LOGGER.info("{} server init ddal-engine from {}", getServerName(), args.configFile);
            engine = (Engine)SessionFactoryBuilder.newBuilder().fromXml(args.configFile).build();
            LOGGER.info("{} server ddal-engine inited.", getServerName());
        } catch (Exception e) {
            LOGGER.error("Exception happen when init ddal-engine ", e);
            ServerException convert = ServerException.convert(e);
            throw convert;
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
            LOGGER.error("Exception happen when start " + getServerName(), e);
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
        uptime = System.currentTimeMillis();
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

        b.childHandler(newChannelInitializer());

        return b;
    }

    private ThreadPoolExecutor createUserThreadExecutor() {
        TaskQueue queue = new TaskQueue(SysProperties.THREAD_QUEUE_SIZE);
        int poolCoreSize = SysProperties.THREAD_POOL_SIZE_CORE;
        int poolMaxSize = SysProperties.THREAD_POOL_SIZE_MAX;
        poolMaxSize = poolMaxSize > poolCoreSize ? poolMaxSize : poolCoreSize;
        ExtendableThreadPoolExecutor userExecutor = new ExtendableThreadPoolExecutor(poolCoreSize, poolMaxSize, 5L,
                TimeUnit.MINUTES, queue, Threads.newThreadFactory("request-processor"));
        return userExecutor;
    }


    protected abstract String getServerName();

    protected abstract ChannelHandler newChannelInitializer();
    
    public abstract QueryDispatcher newQueryDispatcher(ServerSession session);

    class ShutdownThread extends Thread {
        @Override
        public void run() {
            NettyServer.this.stop();
        }
    }
}
