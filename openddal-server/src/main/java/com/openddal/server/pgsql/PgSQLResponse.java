package com.openddal.server.pgsql;

import java.io.OutputStream;
import java.sql.ResultSet;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.Response;
import com.openddal.server.mysql.proto.ERR;

import io.netty.buffer.ByteBuf;

public class PgSQLResponse implements Response {
    
    private final ProtocolTransport trans;

    public PgSQLResponse(ProtocolTransport trans) {
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


    /* (non-Javadoc)
     * @see com.openddal.server.Response#sendResultSet(java.sql.ResultSet)
     */
    @Override
    public void sendResultSet(ResultSet resultSet) {
        // TODO Auto-generated method stub
        
    }

    
}
