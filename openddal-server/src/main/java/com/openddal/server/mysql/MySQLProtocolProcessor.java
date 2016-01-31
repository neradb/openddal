package com.openddal.server.mysql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.Request;
import com.openddal.server.Response;
import com.openddal.server.mysql.parser.ServerParse;
import com.openddal.server.mysql.proto.Com_Query;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.OK;
import com.openddal.server.mysql.proto.Packet;
import com.openddal.server.processor.AbstractProtocolProcessor;
import com.openddal.server.processor.ProtocolProcessException;

import io.netty.buffer.ByteBuf;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLProtocolProcessor extends AbstractProtocolProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtocolProcessor.class);

    @Override
    protected void doProcess(Request request, Response response) throws ProtocolProcessException {
        
        ByteBuf buffer = request.getInputBuffer();
        byte[] packet = new byte[buffer.readableBytes()];
        buffer.readBytes(packet);
        //long sequenceId = Packet.getSequenceId(packet);
        byte type = Packet.getType(packet);
        
        switch (type) {
        case Flags.COM_INIT_DB:
            sendOk();
            break;
        case Flags.COM_QUERY:
            String query = Com_Query.loadFromPacket(packet).query;
            query(query);
            break;
        case Flags.COM_PING:
            sendOk();
            break;
        case Flags.COM_QUIT:
            request.getSession().close();
            break;
        case Flags.COM_PROCESS_KILL:
        case Flags.COM_STMT_PREPARE:
        case Flags.COM_STMT_EXECUTE:
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
            throw new ProtocolProcessException(MySQLErrorCode.ER_NOT_SUPPORTED_YET, "Command not supported yet");
        default:
            throw new ProtocolProcessException(MySQLErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void query(String sql) throws ProtocolProcessException {
        if (sql == null || sql.length() == 0) {
            throw new ProtocolProcessException(MySQLErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new StringBuilder().append(this).append(" ").append(sql).toString());
        }
        // remove last ';'
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }

        // 执行查询
        int rs = ServerParse.parse(sql);
        int sqlType = rs & 0xff;
        switch (sqlType) {
        case ServerParse.EXPLAIN:
            handleExplain(sql, rs >>> 8);
            break;
        case ServerParse.SET:
            handleSet(sql, rs >>> 8);
            break;
        case ServerParse.SHOW:
            handleShow(sql, rs >>> 8);
            break;
        case ServerParse.SELECT:
            handleSelect(sql, rs >>> 8);
            break;
        case ServerParse.START:
            handleStart(sql, rs >>> 8);
            break;
        case ServerParse.BEGIN:
            handleBegin(sql, rs >>> 8);
            break;
        case ServerParse.SAVEPOINT:
            handleSavepoint(sql, rs >>> 8 );
            break;
        case ServerParse.KILL:
            handleKill(sql, rs >>> 8);
            break;
        case ServerParse.KILL_QUERY:
            LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
            throw ProtocolProcessException.get(MySQLErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
        case ServerParse.USE:
            handleUse(sql, rs >>> 8);
            break;
        case ServerParse.COMMIT:
            handleComment(sql, rs >>> 8);
            break;
        case ServerParse.ROLLBACK:
            handleRollback(sql, rs >>> 8);
            break;
        case ServerParse.HELP:
            LOGGER.warn(new StringBuilder().append("Unsupported command:").append(sql).toString());
            throw ProtocolProcessException.get(MySQLErrorCode.ER_SYNTAX_ERROR, "Unsupported command");

        case ServerParse.MYSQL_CMD_COMMENT:
           
            break;
        case ServerParse.MYSQL_COMMENT:
            break;
        case ServerParse.LOAD_DATA_INFILE_SQL:
            break;
        default:
            throw ProtocolProcessException.get(MySQLErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command"); 
        }
    }

    public void handleExplain(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleSet(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleShow(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleSelect(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleStart(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleBegin(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleSavepoint(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleKill(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleQuery(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleUse(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleCommit(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleRollback(String stmt, int offset) {
        stmt = stmt.substring(offset);

    }

    public void handleHelp(String stmt, int offset) {
        stmt = stmt.substring(offset);
    }

    public void handleComment(String stmt, int offset) {
        stmt = stmt.substring(offset);
    }
    
    
    private void sendOk() {
        ByteBuf out = getResponse().getOutputBuffer();
        OK ok = new OK();
        out.writeBytes(ok.toPacket());
    }
}
