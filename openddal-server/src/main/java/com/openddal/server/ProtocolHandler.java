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

import com.openddal.server.processor.*;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ProtocolHandler extends ChannelInboundHandlerAdapter {
    private static Logger logger = LoggerFactory.getLogger(ProtocolHandler.class);

    private final ProcessorFactory processorFactory;
    private final Executor userExecutor;

    public ProtocolHandler(Executor executor) {
        this.processorFactory = new ProcessorFactory();
        this.userExecutor = executor;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        ProtocolTransport transport = new ProtocolTransport(ctx, (ByteBuf) msg);
        userExecutor.execute(new ProcessorTask(ctx, transport));
    }

    /**
     * Execute the thrift processor and user code in user threads.
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
            Request request = RequestFactory.getInstance().createRequest(transport);
            Response response = ResponseFactory.getInstance().createResponse(transport);
            try {
                processorFactory.getProcessor(transport).process(request, response);
                ctx.writeAndFlush(transport.out);
            } catch (ProcessException e) {
                logger.error("process exception happen when call processor", e);
                // TODO: response thrift wrong exception,
            } catch (Exception e) {
                logger.error("User exception happen when call processor", e);
                // TODO: response user wrong exception,
            } finally {
                transport.in.release();
            }
        }
    }
}
