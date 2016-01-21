package com.openddal.server.mysql.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.Request;
import com.openddal.server.Response;
import com.openddal.server.Session;
import com.openddal.server.mysql.ErrorCode;
import com.openddal.server.mysql.parser.ServerParse;
import com.openddal.server.mysql.proto.Com_Query;
import com.openddal.server.mysql.proto.Packet;
import com.openddal.server.processor.AbstractProtocolProcessor;
import com.openddal.server.processor.ProtocolProcessException;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLQueryProcessor extends AbstractProtocolProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLQueryProcessor.class);

    @Override
    protected void doProcess(Request request, Response response) throws ProtocolProcessException {
        try {
            byte[] packet = Packet.read_packet(request.getInputStream());
            long sequenceId = Packet.getSequenceId(packet);
            String query = Com_Query.loadFromPacket(packet).query;
            query(query);
        } catch (Exception e) {
            throw ProtocolProcessException.convert(e);
        }
    }

    public void query(String sql) throws ProtocolProcessException {
        if (sql == null || sql.length() == 0) {
            throw new ProtocolProcessException(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
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
            throw ProtocolProcessException.get(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command");
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
            throw ProtocolProcessException.get(ErrorCode.ER_SYNTAX_ERROR, "Unsupported command");

        case ServerParse.MYSQL_CMD_COMMENT:
           
            break;
        case ServerParse.MYSQL_COMMENT:
            break;
        case ServerParse.LOAD_DATA_INFILE_SQL:
            break;
        default:
            throw ProtocolProcessException.get(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported command"); 
        }
    }

    public void handleExplain(String stmt, int offset) {
        Session session = getSession();
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
}
