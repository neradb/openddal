package com.openddal.server.mysql.processor;

import java.util.Map;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.mysql.ErrorCode;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.ProtocolProcessException;
import com.openddal.server.processor.ProtocolProcessor;
import com.openddal.util.New;

public class MySQLProcessorFactory implements ProcessorFactory {
    
    private final Map<Byte,ProtocolProcessor> processors = New.hashMap();
    
    
    
    
    public MySQLProcessorFactory() {
        processors.put(Flags.COM_INIT_DB, new MySQLInitDbProcessor());
        processors.put(Flags.COM_QUERY, new MySQLQueryProcessor());
        processors.put(Flags.COM_PING, new MySQLPingProcessor());
        processors.put(Flags.COM_QUIT, new MySQLQuitProcessor());
        processors.put(Flags.COM_PROCESS_KILL, new MySQLKillProcessor());
        processors.put(Flags.COM_STMT_PREPARE, new MySQLPrepareProcessor());
        processors.put(Flags.COM_STMT_EXECUTE, new MySQLExecuteProcessor());
        processors.put(Flags.COM_STMT_CLOSE, new MySQLStmtCloseProcessor());
    }




    @Override
    public ProtocolProcessor getProcessor(ProtocolTransport trans) throws ProtocolProcessException {
        ProtocolProcessor result = null;
        byte type = trans.in.getByte(4);
        switch (type) {
        case Flags.COM_INIT_DB:
        case Flags.COM_QUERY:
        case Flags.COM_PING:
        case Flags.COM_QUIT:
        case Flags.COM_PROCESS_KILL:
        case Flags.COM_STMT_PREPARE:
        case Flags.COM_STMT_EXECUTE:
        case Flags.COM_STMT_CLOSE:
            result = processors.get(type);
            break;

        case Flags.COM_SLEEP:// deprecated
        case Flags.COM_FIELD_LIST:
        case Flags.COM_CREATE_DB:
        case Flags.COM_DROP_DB:
        case Flags.COM_REFRESH:
        case Flags.COM_SHUTDOWN:
        case Flags.COM_STATISTICS:
        case Flags.COM_PROCESS_INFO: // deprecated
        case Flags.COM_CONNECT:// deprecated
        case Flags.COM_DEBUG:
        case Flags.COM_TIME:// deprecated
        case Flags.COM_DELAYED_INSERT:// deprecated
        case Flags.COM_CHANGE_USER:
        case Flags.COM_BINLOG_DUMP:
        case Flags.COM_TABLE_DUMP:
        case Flags.COM_CONNECT_OUT:
        case Flags.COM_REGISTER_SLAVE:
        case Flags.COM_STMT_SEND_LONG_DATA:
        case Flags.COM_STMT_RESET:
        case Flags.COM_SET_OPTION:
        case Flags.COM_STMT_FETCH:
        case Flags.COM_DAEMON: // deprecated
        case Flags.COM_BINLOG_DUMP_GTID:
        case Flags.COM_END:
            throw new ProtocolProcessException(ErrorCode.ER_NOT_SUPPORTED_YET, "Command not supported yet");
        default:
            throw new ProtocolProcessException(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
        if(result == null) {
            throw new ProtocolProcessException(ErrorCode.ER_UNKNOWN_STMT_HANDLER, "No processor for command");
        }
        return result;

    }

}
