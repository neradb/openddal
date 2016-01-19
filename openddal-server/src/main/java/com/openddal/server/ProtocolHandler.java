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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.ProtocolProcessException;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
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

    private final Authenticator authenticator;
    private final ProcessorFactory processorFactory;
    private final RequestFactory requestFactory;
    private final ResponseFactory responseFactory;
    private final ThreadPoolExecutor userExecutor;

    public ProtocolHandler(Authenticator authenticator, 
            ProcessorFactory processorFactory,
            RequestFactory requestFactory, 
            ResponseFactory responseFactory, 
            ThreadPoolExecutor executor) {
        this.authenticator = authenticator;
        this.processorFactory = processorFactory;
        this.requestFactory = requestFactory;
        this.responseFactory = responseFactory;
        this.userExecutor = executor;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        authenticator.onConnected(ctx.channel());
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ByteBuf buf = (ByteBuf) msg;
        Channel channel = ctx.channel();
        Session session = channel.attr(Session.CHANNEL_SESSION_KEY).get();
        if(session == null) {
            authenticator.authorize(channel, buf);
        } else {
            ProtocolTransport transport = new ProtocolTransport(channel, buf);
            userExecutor.execute(new ProcessorTask(ctx, transport));
        }
        
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
