package com.openddal.server;

import java.util.concurrent.ThreadPoolExecutor;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
@Sharable
public abstract class HandshakeHandler extends ChannelInboundHandlerAdapter {

    protected ThreadPoolExecutor userExecutor;

    public ThreadPoolExecutor getUserExecutor() {
        return userExecutor;
    }

    public void setUserExecutor(ThreadPoolExecutor userExecutor) {
        this.userExecutor = userExecutor;
    }

}
