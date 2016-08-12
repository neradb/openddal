/*-------------------------------------------------------------------------------------------------
 _______ __   _ _______ _______ ______  ______
 |_____| | \  |    |    |______ |     \ |_____]
 |     | |  \_|    |    ______| |_____/ |_____]

 Copyright (c) 2016, antsdb.com and/or its affiliates. All rights reserved. *-xguo0<@

 This program is free software: you can redistribute it and/or modify it under the terms of the
 GNU Affero General Public License, version 3, as published by the Free Software Foundation.

 You should have received a copy of the GNU Affero General Public License along with this program.
 If not, see <https://www.gnu.org/licenses/agpl-3.0.txt>
-------------------------------------------------------------------------------------------------*/
package com.openddal.server.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.NettyServer;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public class MySQLChannelInitializer extends ChannelInitializer<SocketChannel> {
    private static final Logger logger = LoggerFactory.getLogger(MySQLChannelInitializer.class);
    
    private NettyServer server;
    
    public MySQLChannelInitializer(NettyServer server) {
        this.server = server;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        if(logger.isDebugEnabled()) {
            logger.debug("channel initialized with remote address {}", ch.remoteAddress());
        }
        MysqlServerHandler handler = new MysqlServerHandler(server);
        PacketDecoder decoder = new PacketDecoder(handler);
        ch.pipeline().addLast(decoder, handler);
    }
}
