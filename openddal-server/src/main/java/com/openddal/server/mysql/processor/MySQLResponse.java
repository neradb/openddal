package com.openddal.server.mysql.processor;

import java.io.OutputStream;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.processor.Response;

import io.netty.buffer.ByteBuf;

public class MySQLResponse implements Response {
    
    private final ProtocolTransport trans;

    public MySQLResponse(ProtocolTransport trans) {
        this.trans = trans;
    }


    @Override
    public void sendError(int errno, String msg) {
        ERR err = new ERR();
        err.errorCode = errno;
        err.errorMessage = msg;
        trans.out.writeBytes(err.toPacket());
    }
    

    @Override
    public OutputStream getOutputStream() {
        return trans.getOutputStream();
    }
    

    @Override
    public ByteBuf getOutputBuffer() {
        return trans.out;
    }

    
}
