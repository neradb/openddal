package com.openddal.server.mysql.processor;
import com.openddal.server.mysql.packet.MySQLPacket;
import com.openddal.server.ProtocolTransport;
import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.ProtocolProcessor;

public class MySQLProcessorFactory implements ProcessorFactory {
    @Override
    public ProtocolProcessor getProcessor(ProtocolTransport trans) {
        byte type = trans.in.readByte();
        switch (type) {
            case MySQLPacket.COM_INIT_DB:
                break;
            case MySQLPacket.COM_QUERY:
                break;
            case MySQLPacket.COM_PING:
                break;
            case MySQLPacket.COM_QUIT:
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                break;
            case MySQLPacket.COM_STMT_CLOSE:
                break;
            case MySQLPacket.COM_HEARTBEAT:
                break;
            default:
                //source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
        return null;

    }

}
