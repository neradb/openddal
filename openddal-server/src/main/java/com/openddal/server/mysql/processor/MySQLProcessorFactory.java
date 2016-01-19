package com.openddal.server.mysql.processor;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.ProtocolProcessor;

public class MySQLProcessorFactory implements ProcessorFactory {
    @Override
    public ProtocolProcessor getProcessor(ProtocolTransport trans) {
        byte type = trans.in.getByte(4);
        switch (type) {
        case Flags.COM_INIT_DB:
            break;
        case Flags.COM_QUERY:
            break;
        case Flags.COM_PING:
            break;
        case Flags.COM_QUIT:
            break;
        case Flags.COM_PROCESS_KILL:
            break;
        case Flags.COM_STMT_PREPARE:
            break;
        case Flags.COM_STMT_EXECUTE:
            break;
        case Flags.COM_STMT_CLOSE:
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
        default:
            // source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown
            // command");
        }
        return null;

    }

}
