package com.openddal.server.mysql;

import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.ProtocolProcessException;
import com.openddal.server.ProtocolProcessor;
import com.openddal.server.ProtocolTransport;
import com.openddal.server.Session;
import com.openddal.server.mysql.parser.ServerParse;
import com.openddal.server.mysql.parser.ServerParseSelect;
import com.openddal.server.mysql.parser.ServerParseSet;
import com.openddal.server.mysql.parser.ServerParseShow;
import com.openddal.server.mysql.parser.ServerParseStart;
import com.openddal.server.mysql.proto.Com_Query;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.OK;
import com.openddal.server.mysql.proto.Packet;
import com.openddal.server.mysql.respo.CharacterSet;
import com.openddal.server.util.ErrorCode;
import com.openddal.util.StringUtils;

import io.netty.buffer.ByteBuf;

public class MySQLProtocolProcessor implements ProtocolProcessor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtocolProcessor.class);
    private static final Logger accessLogger = LoggerFactory.getLogger("AccessLogger");
    
    private static ThreadLocal<ProtocolTransport> transportHolder = new ThreadLocal<ProtocolTransport>();
    private static ThreadLocal<Session> sessionHolder = new ThreadLocal<Session>();
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    @Override
    public final boolean process(ProtocolTransport transport) throws ProtocolProcessException {
        ProtocolProcessException e = null;
        try {
            transportHolder.set(transport);
            sessionHolder.set(transport.getSession());
            connHolder.set(transport.getSession().getEngineConnection());
            doProcess(transport);
        } catch (Exception ex) {
            e = ProtocolProcessException.convert(ex);
            throw e;
        } finally {
            sessionHolder.remove();
            transportHolder.remove();
            connHolder.remove();
        }
        return e == null;
       
    }
    
    
    public final Session getSession() {
        return sessionHolder.get();
    }
    public final Connection getConnection() {
        return connHolder.get();
    }
    
    public final ProtocolTransport getProtocolTransport() {
        return transportHolder.get();
    }
    




    protected void doProcess(ProtocolTransport transport) throws Exception {
        
        ByteBuf buffer = transport.in;
        byte[] packet = new byte[buffer.readableBytes()];
        buffer.readBytes(packet);
        long sequenceId = Packet.getSequenceId(packet);
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
            sendError(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
            return;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(new StringBuilder().append(this).append(" ").append(sql).toString());
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


    private void processStart(String sql, int offset) {
        switch (ServerParseStart.parse(sql, offset)) {
            case ServerParseStart.TRANSACTION:
                unsupported("");
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
            case ServerParseSet.SQL_MODE:
            case ServerParseSet.AT_VAR:
                execute(stmt, ServerParse.SET);
                break;
            default:
                StringBuilder s = new StringBuilder();
                LOGGER.warn(s.append(stmt).append(" is not executed").toString());
                sendOk();
        }
    }


    public static void processShow(String stmt, int offset) {
        switch (ServerParseShow.parse(stmt, offset)) {
            case ServerParseShow.DATABASES:
                //ShowDatabases.response(c);
                break;

            case ServerParseShow.CONNECTION:
                //ShowConnection.execute(c);
                break;
            //            case ServerParseShow.DATASOURCES:
            //                // ShowDataSources.response(c);
            //                // break;
            //            case ServerParseShow.COBAR_STATUS:
            //                // ShowCobarStatus.response(c);
            //                // break;
            case ServerParseShow.SLOW:
                //ShowSQLSlow.execute(c);
                break;
            case ServerParseShow.PHYSICAL_SLOW:
                //ShowPhysicalSQLSlow.execute(c);
                break;
            default:
                execute(stmt, ServerParse.SHOW);
        }
    }
    
    
    
    public static void processSelect(String stmt, int offs) {
        int offset = offs;
        switch (ServerParseSelect.parse(stmt, offs)) {
            case ServerParseSelect.VERSION_COMMENT:
                //SelectVersionComment.response(c);
                break;
            case ServerParseSelect.DATABASE:
                //SelectDatabase.response(c);
                break;
            case ServerParseSelect.USER:
                //SelectUser.response(c);
                break;
            case ServerParseSelect.VERSION:
                //SelectVersion.response(c);
                break;
            case ServerParseSelect.LAST_INSERT_ID:
                break;
            case ServerParseSelect.IDENTITY:
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


    private static void execute(String stmt, int offset) {
        
    }
    
    private void unsupported(String msg) {
        sendError(ErrorCode.ER_UNKNOWN_COM_ERROR, msg);
    }
    
    public void sendOk() {
        OK ok = new OK();
        getProtocolTransport().out.writeBytes(ok.toPacket());
    }

    public void sendError(int errno, String msg) {
        ERR err = new ERR();
        err.errorCode = errno;
        err.errorMessage = msg;
        getProtocolTransport().out.writeBytes(err.toPacket());
    }
    
}
