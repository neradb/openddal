package com.openddal.server.mysql.processor;

import java.io.UnsupportedEncodingException;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.mysql.packet.ErrorPacket;
import com.openddal.server.processor.Response;
import com.openddal.server.processor.Session;
import com.openddal.server.processor.SessionImpl;

import io.netty.buffer.ByteBuf;

public class MySQLResponse implements Response {
    
    private final ProtocolTransport trans;

    public MySQLResponse(ProtocolTransport trans) {
        this.trans = trans;
    }

    
    @Override
    public void sendError(int sc) {
        sendError(sc, null);
    }

    @Override
    public void sendError(int errno, String msg) {
        trans.out.clear();
        ErrorPacket err = new ErrorPacket();
        err.packetId = 1;
        err.errno = errno;
        err.message = encodeString(msg);
        err.write(trans.out);            
    }


    @Override
    public ByteBuf getOutputBuffer() {
        return trans.out;
    }
    
    private String getCharset() {
        Session session = this.trans.attr(SessionImpl.SESSION_KEY).get();
        if(session != null) {
            return session.getCharset();
        }
        return null;
    }
    
    private final byte[] encodeString(String src) {
        if (src == null) {
            return null;
        }
        String charset = getCharset();
        if (charset == null) {
            return src.getBytes();
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }
    
}
