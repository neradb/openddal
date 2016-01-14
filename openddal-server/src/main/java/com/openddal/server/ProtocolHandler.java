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

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.ProtocolProcessException;
import com.openddal.server.processor.Request;
import com.openddal.server.processor.RequestFactory;
import com.openddal.server.processor.Response;
import com.openddal.server.processor.ResponseFactory;
import com.openddal.server.processor.Session;
import com.openddal.server.processor.SessionImpl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
@Sharable
public class ProtocolHandler extends ChannelInboundHandlerAdapter {
    private static final Logger logger = LoggerFactory.getLogger(ProtocolHandler.class);

    private final ProcessorFactory processorFactory;
    private final RequestFactory requestFactory;
    private final ResponseFactory responseFactory;
    private final ThreadPoolExecutor userExecutor;
    private final AtomicLong sessionIdGenerator = new AtomicLong(0);

    public ProtocolHandler(ProcessorFactory processorFactory, RequestFactory requestFactory,
            ResponseFactory responseFactory, ThreadPoolExecutor executor) {
        this.processorFactory = processorFactory;
        this.requestFactory = requestFactory;
        this.responseFactory = responseFactory;
        this.userExecutor = executor;
    }


    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteBuf in = Unpooled.wrappedBuffer(new byte[0]);
        ProtocolTransport transport = new ProtocolTransport(ctx.channel(), in);
        SessionImpl session = new SessionImpl(sessionIdGenerator.incrementAndGet());
        Session old = ctx.attr(SessionImpl.SESSION_KEY).get();
        if(old != null) {
            throw new IllegalStateException("session is already existing in channel");
        }
        ctx.attr(SessionImpl.SESSION_KEY).set(session);        
        userExecutor.execute(new ProcessorTask(ctx, transport));
        //ctx.read();
    }


    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ProtocolTransport transport = new ProtocolTransport(ctx.channel(), (ByteBuf) msg);
        userExecutor.execute(new ProcessorTask(ctx, transport));
    }
    

    /**
     * Execute the processor in user threads.
     */
    class ProcessorTask implements Runnable {
        private ChannelHandlerContext ctx;
        private ProtocolTransport transport;

        ProcessorTask(ChannelHandlerContext ctx, ProtocolTransport transport) {
            this.ctx = ctx;
            this.transport = transport;
        }

        @Override
        public void run() {
            Request request = requestFactory.createRequest(transport);
            Response response = responseFactory.createResponse(transport);
            try {
                processorFactory.getProcessor(transport).process(request, response);
            } catch (ProtocolProcessException e) {
                logger.error("process exception happen when call processor", e);
                response.sendError(e.getErrorCode(), e.getErrorMessage());
            } catch (Throwable e) {
                logger.error("User exception happen when call processor", e);
                ProtocolProcessException convert = ProtocolProcessException.convert(e);
                response.sendError(convert.getErrorCode(), convert.getErrorMessage());
            } finally {
                ctx.writeAndFlush(transport.out);
                transport.in.release();
            }
        }
    }
}
