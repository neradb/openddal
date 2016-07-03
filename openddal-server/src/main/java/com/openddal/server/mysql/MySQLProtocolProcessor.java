package com.openddal.server.mysql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.ProtocolProcessException;
import com.openddal.server.ProtocolTransport;
import com.openddal.server.TraceableProcessor;
import com.openddal.server.mysql.parser.ServerParse;
import com.openddal.server.mysql.parser.ServerParseSelect;
import com.openddal.server.mysql.parser.ServerParseSet;
import com.openddal.server.mysql.parser.ServerParseShow;
import com.openddal.server.mysql.parser.ServerParseStart;
import com.openddal.server.mysql.proto.ColumnPacket;
import com.openddal.server.mysql.proto.Com_Query;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.OK;
import com.openddal.server.mysql.proto.Packet;
import com.openddal.server.mysql.proto.ResultSetPacket;
import com.openddal.server.mysql.proto.RowPacket;
import com.openddal.server.mysql.respo.CharacterSet;
import com.openddal.server.mysql.respo.SelectVariables;
import com.openddal.server.mysql.respo.ShowVariables;
import com.openddal.server.mysql.respo.ShowVersion;
import com.openddal.server.util.ErrorCode;
import com.openddal.server.util.MysqlDefs;
import com.openddal.server.util.ResultSetUtil;
import com.openddal.server.util.StringUtil;
import com.openddal.util.JdbcUtils;
import com.openddal.util.StringUtils;

import io.netty.buffer.ByteBuf;

public class MySQLProtocolProcessor extends TraceableProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtocolProcessor.class);

    protected void doProcess(ProtocolTransport transport) throws Exception {

        ByteBuf buffer = transport.in;
        byte[] packet = new byte[buffer.readableBytes()];
        buffer.readBytes(packet);
        byte type = Packet.getType(packet);

        switch (type) {
        case Flags.COM_INIT_DB:
            sendOk();
            break;
        case Flags.COM_QUERY:
            String query = Com_Query.loadFromPacket(packet).query;
            getTrace().protocol("COM_QUERY").sql(query);
            query(query);
            break;
        case Flags.COM_PING:
            getTrace().protocol("COM_PING");
            sendOk();
            break;
        case Flags.COM_QUIT:
            getTrace().protocol("COM_QUIT");
            getSession().close();
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
            throw new ProtocolProcessException(ErrorCode.ER_NOT_SUPPORTED_YET, "Command not supported yet");
        default:
            throw new ProtocolProcessException(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
    }

    public void query(String sql) throws Exception {
        if (StringUtils.isNullOrEmpty(sql)) {
            throw throwError(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
        }

        int rs = ServerParse.parse(sql);
        switch (rs & 0xff) {
        case ServerParse.SET:
            processSet(sql, rs >>> 8);
            break;
        case ServerParse.SHOW:
            processShow(sql, rs >>> 8);
            break;
        case ServerParse.SELECT:
            processSelect(sql, rs >>> 8);
            break;
        case ServerParse.START:
            processStart(sql, rs >>> 8);
            break;
        case ServerParse.BEGIN:
            processBegin(sql, rs >>> 8);
            break;
        case ServerParse.LOAD:
            processSavepoint(sql, rs >>> 8);
            break;
        case ServerParse.SAVEPOINT:
            processSavepoint(sql, rs >>> 8);
            break;
        case ServerParse.USE:
            processUse(sql, rs >>> 8);
            break;
        case ServerParse.COMMIT:
            processCommit(sql, rs >>> 8);
            break;
        case ServerParse.ROLLBACK:
            processRollback(sql, rs >>> 8);
            break;
        default:
            execute(sql, rs);
        }
    }

    private void processCommit(String sql, int i) {
        // TODO Auto-generated method stub

    }

    private void processRollback(String sql, int i) {
        // TODO Auto-generated method stub

    }

    private void processUse(String sql, int i) {
        // TODO Auto-generated method stub

    }

    private void processBegin(String sql, int i) {
        // TODO Auto-generated method stub

    }

    private void processSavepoint(String sql, int i) {
        // TODO Auto-generated method stub

    }

    private void processStart(String sql, int offset) throws Exception {
        switch (ServerParseStart.parse(sql, offset)) {
        case ServerParseStart.TRANSACTION:
            unsupported("Start TRANSACTION");
            break;
        default:
            execute(sql, ServerParse.START);
        }

    }

    public void processSet(String stmt, int offset) throws Exception {
        Connection c = getConnection();
        int rs = ServerParseSet.parse(stmt, offset);
        switch (rs & 0xff) {
        case ServerParseSet.AUTOCOMMIT_ON:
            if (!c.getAutoCommit()) {
                c.setAutoCommit(true);
            }
            sendOk();
            break;
        case ServerParseSet.AUTOCOMMIT_OFF: {
            if (c.getAutoCommit()) {
                c.setAutoCommit(false);
            }
            sendOk();
            break;
        }
        case ServerParseSet.TX_READ_UNCOMMITTED: {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            sendOk();
            break;
        }
        case ServerParseSet.TX_READ_COMMITTED: {
            c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            sendOk();
            break;
        }
        case ServerParseSet.TX_REPEATED_READ: {
            c.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            sendOk();
            break;
        }
        case ServerParseSet.TX_SERIALIZABLE: {
            c.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            sendOk();
            break;
        }
        case ServerParseSet.NAMES:
            String charset = stmt.substring(rs >>> 8).trim();
            if (getSession().setCharset(charset)) {
                sendOk();
            } else {
                sendError(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
            }
            break;
        case ServerParseSet.CHARACTER_SET_CLIENT:
        case ServerParseSet.CHARACTER_SET_CONNECTION:
        case ServerParseSet.CHARACTER_SET_RESULTS:
            CharacterSet.response(stmt, this, rs);
            break;
        case ServerParseSet.AT_VAR:
            execute(stmt, ServerParse.SET);
            break;
        default:
            StringBuilder s = new StringBuilder();
            LOGGER.warn(s.append(stmt).append(" is not executed").toString());
            sendOk();
        }
    }

    public void processShow(String stmt, int offset) throws Exception {
        switch (ServerParseShow.parse(stmt, offset)) {
        case ServerParseShow.DATABASES:
            // ShowDatabases.response(c);
            break;

        case ServerParseShow.CONNECTION:
            // ShowConnection.execute(c);
            break;
        case ServerParseShow.SLOW:
            // ShowSQLSlow.execute(c);
            break;
        case ServerParseShow.PHYSICAL_SLOW:
            // ShowPhysicalSQLSlow.execute(c);
            break;
        case ServerParseShow.VARIABLES:
            sendResultSet(ShowVariables.getResultSet());
            break;
        case ServerParseShow.SESSION_VARIABLES:
            sendResultSet(SelectVariables.getResultSet(stmt));
            break;
        default:
            execute(stmt, ServerParse.SHOW);
        }
    }

    public void processSelect(String stmt, int offs) throws Exception {
        switch (ServerParseSelect.parse(stmt, offs)) {
        case ServerParseSelect.VERSION_COMMENT:
            sendResultSet(ShowVersion.getCommentResultSet());
            break;
        case ServerParseSelect.DATABASE:
            execute("SELECT SCHEMA()", ServerParse.SELECT);
            break;
        case ServerParseSelect.CONNECTION_ID:
            execute("SELECT SESSION_ID()", ServerParse.SELECT);
            break;
        case ServerParseSelect.USER:
            execute("SELECT USER()", ServerParse.SELECT);
            break;
        case ServerParseSelect.VERSION:
            sendResultSet(ShowVersion.getResultSet());
            break;
        case ServerParseSelect.LAST_INSERT_ID:
            execute("SELECT LAST_INSERT_ID()", ServerParse.SELECT);
            break;
        case ServerParseSelect.IDENTITY:
            execute("SELECT SCOPE_IDENTITY()", ServerParse.SELECT);
            break;
        case ServerParseSelect.SELECT_SESSION_VARIABLES:
            sendResultSet(SelectVariables.getResultSet(stmt));
            break;
        default:
            execute(stmt, ServerParse.SELECT);
        }
    }

    public void processKill(String stmt, int offset) {
        String id = stmt.substring(offset).trim();
        if (StringUtils.isNullOrEmpty(id)) {
            sendError(ErrorCode.ER_NO_SUCH_THREAD, "NULL connection id");
        } else {
            // get value
            long value = 0;
            try {
                value = Long.parseLong(id);
            } catch (NumberFormatException e) {
                sendError(ErrorCode.ER_NO_SUCH_THREAD, "Invalid connection id:" + id);
                return;
            }
            sendError(ErrorCode.ER_NO_SUCH_THREAD, "Unknown connection id:" + id);
        }
    }

    private void execute(String sql, int type) throws Exception {
        Connection conn = getSession().getEngineConnection();
        Statement stmt = null;
        ResultSet rs = null;
        switch (type) {
        case ServerParse.SELECT:
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery(sql);
                sendResultSet(rs);
            } finally {
                JdbcUtils.closeSilently(stmt);
                JdbcUtils.closeSilently(rs);
            }
            break;
        case ServerParse.SET:
            try {
                stmt = conn.createStatement();
                stmt.execute(sql);
                sendOk();
            } finally {
                JdbcUtils.closeSilently(stmt);
                JdbcUtils.closeSilently(rs);
            }
            break;

        default:
            unsupported(sql + " unsupported.");
        }
    }

    private void unsupported(String msg) {
        sendError(ErrorCode.ER_UNKNOWN_COM_ERROR, msg);
    }

    public void sendOk() {
        OK ok = new OK();
        ok.sequenceId = 1;
        ok.setStatusFlag(Flags.SERVER_STATUS_AUTOCOMMIT);
        getProtocolTransport().out.writeBytes(ok.toPacket());
    }

    public void sendError(int errno, String msg) {
        ERR err = new ERR();
        err.errorCode = errno;
        err.errorMessage = msg;
        getProtocolTransport().out.writeBytes(err.toPacket());
    }

    public Exception throwError(int errno, String msg) {
        return new ProtocolProcessException(errno, msg);
    }

    /**
     * @see https://dev.mysql.com/doc/internals/en/com-query-response.html
     * 
     * @param rs
     * @throws Exception
     */
    public void sendResultSet(ResultSet rs) throws Exception {
        int charsetIndex = getSession().getCharsetIndex();
        ResultSetMetaData metaData = rs.getMetaData();
        int colunmCount = metaData.getColumnCount();

        ResultSetPacket resultset = new ResultSetPacket();
        ResultSetPacket.characterSet = charsetIndex;

        for (int i = 0; i < colunmCount; i++) {
            int j = i + 1;
            ColumnPacket columnPacket = new ColumnPacket();
            columnPacket.org_name = StringUtil.emptyIfNull(metaData.getColumnName(j));
            columnPacket.name = StringUtil.emptyIfNull(metaData.getColumnLabel(j));
            columnPacket.org_table = StringUtil.emptyIfNull(metaData.getTableName(j));
            columnPacket.table = StringUtil.emptyIfNull(metaData.getTableName(j));
            columnPacket.schema = StringUtil.emptyIfNull(metaData.getSchemaName(j));
            columnPacket.flags = ResultSetUtil.toFlag(metaData, j);
            columnPacket.columnLength = metaData.getColumnDisplaySize(j);
            columnPacket.decimals = metaData.getScale(j);
            int javaType = MysqlDefs.javaTypeDetect(metaData.getColumnType(j), (int) columnPacket.decimals);
            columnPacket.type = (byte) (MysqlDefs.javaTypeMysql(javaType) & 0xff);
            resultset.addColumn(columnPacket);
        }

        while (rs.next()) {
            RowPacket rowPacket = new RowPacket();
            for (int i = 0; i < colunmCount; i++) {
                int j = i + 1;
                rowPacket.data.add(rs.getString(j));
            }
        }
        ArrayList<byte[]> packets = resultset.toPackets();
        for (byte[] bs : packets) {
            getProtocolTransport().out.writeBytes(bs);
        }
    }

}
