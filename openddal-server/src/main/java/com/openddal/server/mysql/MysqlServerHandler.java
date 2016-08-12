package com.openddal.server.mysql;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.NettyServer;
import com.openddal.server.ProtocolProcessException;
import com.openddal.server.ProtocolProcessor;
import com.openddal.server.ProtocolTransport;
import com.openddal.server.Session;
import com.openddal.server.SessionImpl;
import com.openddal.server.mysql.MySQLProtocolHandler.HandleTask;
import com.openddal.server.mysql.proto.ComFieldlist;
import com.openddal.server.mysql.proto.ComInitdb;
import com.openddal.server.mysql.proto.ComPing;
import com.openddal.server.mysql.proto.ComProcesskill;
import com.openddal.server.mysql.proto.ComQuery;
import com.openddal.server.mysql.proto.ComQuit;
import com.openddal.server.mysql.proto.ComShutdown;
import com.openddal.server.mysql.proto.ComStmtClose;
import com.openddal.server.mysql.proto.ComStmtExecute;
import com.openddal.server.mysql.proto.ComStmtPrepare;
import com.openddal.server.mysql.proto.ComStmtReset;
import com.openddal.server.mysql.proto.ComStmtSendLongData;
import com.openddal.server.mysql.proto.ERR;
import com.openddal.server.mysql.proto.Flags;
import com.openddal.server.mysql.proto.Handshake;
import com.openddal.server.mysql.proto.HandshakeResponse;
import com.openddal.server.mysql.proto.Packet;
import com.openddal.server.util.CharsetUtil;
import com.openddal.server.util.ErrorCode;
import com.openddal.server.util.StringUtil;
import com.openddal.util.New;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class MysqlServerHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLProtocolHandler.class);
    private static final AtomicLong threadIdGenerator = new AtomicLong(300);
    protected ThreadPoolExecutor userExecutor;

    private final long threadId;
    private NettyServer server;
    private SessionImpl session;
    private PreparedStmtHandler preparedStmtHandler;
    private QueryHandler queryHandler;
    private Map<Integer, MysqlPreparedStatement> prepared = New.hashMap();

    public MysqlServerHandler(NettyServer server) {
        this.threadId = threadIdGenerator.incrementAndGet();
        this.server = server;
        this.userExecutor = server.getUserExecutor();
        this.session = new SessionImpl(server);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        ByteBuf out = ctx.alloc().buffer();
        Handshake handshake = new Handshake();
        handshake.sequenceId = 0;
        handshake.protocolVersion = MySQLServer.PROTOCOL_VERSION;
        handshake.serverVersion = MySQLServer.SERVER_VERSION;
        handshake.connectionId = threadId;
        handshake.challenge1 = getRandomString(8);
        handshake.characterSet = CharsetUtil.getIndex(MySQLServer.DEFAULT_CHARSET);
        handshake.statusFlags = Flags.SERVER_STATUS_AUTOCOMMIT;
        handshake.challenge2 = getRandomString(12);
        handshake.authPluginDataLength = 21;
        handshake.authPluginName = Flags.MYSQL_NATIVE_PASSWORD;
        handshake.capabilityFlags = Flags.CLIENT_BASIC_FLAGS;
        handshake.removeCapabilityFlag(Flags.CLIENT_COMPRESS);
        handshake.removeCapabilityFlag(Flags.CLIENT_SSL);
        handshake.removeCapabilityFlag(Flags.CLIENT_LOCAL_FILES);
        session.setCharsetIndex((int) handshake.characterSet);
        session.setAttachment("seed", handshake.challenge1 + handshake.challenge2);
        out.writeBytes(handshake.toPacket());
        ctx.writeAndFlush(out);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ProtocolTransport transport = new ProtocolTransport(ctx.channel(), (ByteBuf) msg);
        if (transport.getSession() == null) {
            userExecutor.execute(new AuthTask(ctx, transport));
        } else {
            userExecutor.execute(new HandleTask(ctx, transport));
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (this.session != null) {
            for (MysqlPreparedStatement i : this.prepared.values()) {
                i.close();
            }
            this.fish.getOrca().closeSession(this.session);
        }
        super.channelInactive(ctx);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }



    private void query(ChannelHandlerContext ctx, ComQuery request) throws Exception {
        queryHandler.query(ctx, request);
    }

    private void stmtPrepare(ChannelHandlerContext ctx, StmtPreparePacket pstmtPacket) throws SQLException {
        if (preparedStmtHandler != null) {
            String sql = pstmtPacket.sql;
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ctx, ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }

            preparedStmtHandler.prepare(ctx, pstmtPacket);
        } else {
            writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    private void stmtPrepareLongData(ChannelHandlerContext ctx, LongDataPacket dataPacket) throws SQLException {
        if (preparedStmtHandler != null) {
            dataPacket.read_(this);
        } else {
            writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    private void stmtExecute(ChannelHandlerContext ctx, StmtExecutePacket pstmtPacket) {
        if (preparedStmtHandler != null) {
            pstmtPacket.read_(this);
            preparedStmtHandler.execute(ctx, pstmtPacket);
        } else {
            writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    private void fieldList(ChannelHandlerContext ctx, FieldListPacket pstmtPacket) {
        writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Field list unsupported!");
    }

    private void setOption(ChannelHandlerContext ctx, SetOptionPacket pstmtPacket) {
        writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Set option unsupported!");
    }

    private void stmtClose(ChannelHandlerContext ctx, StmtClosePacket pstmtPacket) {
        if (preparedStmtHandler != null) {
            preparedStmtHandler.close(this, pstmtPacket);
        } else {
            writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
        }
    }

    private void close(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(PacketEncoder.OK_PACKET);
    }

    private void ping(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(PacketEncoder.OK_PACKET);
    }

    private void unknown(ChannelHandlerContext ctx) {
        writeErrMessage(ctx, ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void writeErrMessage(ChannelHandlerContext ctx, int errno, String msg) {
        ByteBuf buf = ctx.alloc().buffer();
        PacketEncoder.writePacket(buf, (byte) 1,
                () -> packetEncoder.writeErrorBody(buf, errno, Charset.defaultCharset().encode(msg)));
        ctx.writeAndFlush(buf);
    }

    private Packet readPacket(ByteBuf in) throws ProtocolProcessException {
        Packet result = null;
        byte[] packet = new byte[in.readableBytes()];
        in.readBytes(packet);
        byte type = Packet.getType(packet);
        switch (type) {
        case Flags.COM_INIT_DB:
            result = ComInitdb.loadFromPacket(packet);
            break;
        case Flags.COM_QUERY:
            result = ComQuery.loadFromPacket(packet);
            break;
        case Flags.COM_PING:
            result = ComPing.loadFromPacket(packet);
            break;
        case Flags.COM_QUIT:
            result = ComQuit.loadFromPacket(packet);
            break;
        case Flags.COM_PROCESS_KILL:
            result = ComProcesskill.loadFromPacket(packet);
            break;
        case Flags.COM_STMT_PREPARE:
            result = ComStmtPrepare.loadFromPacket(packet);
            break;
        case Flags.COM_STMT_EXECUTE:
            result = ComStmtExecute.loadFromPacket(packet);
            break;
        case Flags.COM_STMT_CLOSE:
            result = ComStmtClose.loadFromPacket(packet);
            break;
        case Flags.COM_FIELD_LIST:
            result = ComFieldlist.loadFromPacket(packet);
            break;
        case Flags.COM_SHUTDOWN:
            result = ComShutdown.loadFromPacket(packet);
            break;
        case Flags.COM_STMT_SEND_LONG_DATA:
            result = ComStmtSendLongData.loadFromPacket(packet);
            break;
        case Flags.COM_STMT_RESET:
            result = ComStmtReset.loadFromPacket(packet);
            break;
        case Flags.COM_TIME:// deprecated
        case Flags.COM_DELAYED_INSERT:// deprecated
        case Flags.COM_TABLE_DUMP:
        case Flags.COM_REGISTER_SLAVE:
        case Flags.COM_SET_OPTION:
        case Flags.COM_STMT_FETCH:
        case Flags.COM_DAEMON: // deprecated
        case Flags.COM_BINLOG_DUMP_GTID:
        case Flags.COM_END:
        case Flags.COM_SLEEP:// deprecated
        case Flags.COM_CREATE_DB:
        case Flags.COM_DROP_DB:
        case Flags.COM_REFRESH:
        case Flags.COM_PROCESS_INFO: // deprecated
        case Flags.COM_CONNECT:// deprecated
        case Flags.COM_DEBUG:
        case Flags.COM_CHANGE_USER:
        case Flags.COM_STATISTICS:
        case Flags.COM_BINLOG_DUMP:
        case Flags.COM_CONNECT_OUT:
            throw new ProtocolProcessException(ErrorCode.ER_NOT_SUPPORTED_YET, "Command not supported yet");
        default:
            throw new ProtocolProcessException(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
        }
        return result;
    }

    private String getRandomString(int length) {
        char[] chars = new char[length];
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            chars[i] = (char) random.nextInt(127);
        }
        return String.valueOf(chars);
    }

    /**
     * Execute the processor in user threads.
     */
    class AuthTask implements Runnable {
        private ChannelHandlerContext ctx;
        private ProtocolTransport transport;

        AuthTask(ChannelHandlerContext ctx, ProtocolTransport transport) {
            this.ctx = ctx;
            this.transport = transport;
        }

        @Override
        public void run() {
            HandshakeResponse authReply = null;
            try {
                byte[] packet = new byte[transport.in.readableBytes()];
                transport.in.readBytes(packet);
                authReply = HandshakeResponse.loadFromPacket(packet);
                
                if (!authReply.hasCapabilityFlag(Flags.CLIENT_PROTOCOL_41)) {
                    error(ErrorCode.ER_NOT_SUPPORTED_AUTH_MODE, "We do not support Protocols under 4.1");
                    return;
                }
                
                if (!privilege.userExists(authReply.username)) {
                    error(ErrorCode.ER_ACCESS_DENIED_ERROR,
                            "Access denied for user '" + authReply.username + "'");
                    return;
                }
                
                if (!StringUtil.isEmpty(authReply.schema) 
                        && !privilege.schemaExists(authReply.username, authReply.schema)) {
                    String s = "Access denied for user '" + authReply.username
                            + "' to database '" + authReply.schema + "'";
                    error(ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                    return;
                }

                if (!privilege.checkPassword(authReply.username, authReply.authResponse, session.getSeed())) {
                    error(ErrorCode.ER_ACCESS_DENIED_ERROR,
                            "Access denied for user '" + authReply.username + "'");
                    return;
                }
                Connection connect = connectEngine(authReply);
                session.setUser(authReply.username);
                session.setSchema(authReply.schema);
                session.bind(ctx.channel());
                session.setAttachment("remoteAddress", ctx.channel().remoteAddress().toString());
                session.setAttachment("localAddress", ctx.channel().localAddress().toString());
                success(ctx.channel());
            } catch (Exception e) {
                String errMsg = authReply == null ? e.getMessage()
                        : "Access denied for user '" + authReply.username + "' to database '" + authReply.schema + "'";
                LOGGER.error("Authorize failed. " + errMsg, e);
                error(ErrorCode.ER_DBACCESS_DENIED_ERROR, errMsg);
            } finally {
                ctx.writeAndFlush(transport.out);
                transport.in.release();
            }        
        }
        
        public void error(int errno, String msg) {
            transport.out.clear();
            ERR err = new ERR();
            err.sequenceId = 2;
            err.errorCode = errno;
            err.errorMessage = msg;
            transport.out.writeBytes(err.toPacket());
            LOGGER.info(msg);
        } 
        
    }
    
    /**
     * Execute the processor in user threads.
     */
    class HandleTask implements Runnable {
        private ChannelHandlerContext ctx;
        private ProtocolTransport transport;

        HandleTask(ChannelHandlerContext ctx, ProtocolTransport transport) {
            this.ctx = ctx;
            this.transport = transport;
        }

        @Override
        public void run() {
            try {
                ProtocolProcessor processor = processorFactory.getProcessor(transport);
                processor.process(transport);
            } catch (Throwable e) {
                if (!(e.getCause() instanceof SQLException)) {
                    logger.error("an exception happen when process request", e);
                }
                
                handleThrowable(e);
            } finally {
                ctx.writeAndFlush(transport.out);
                transport.in.release();
            }
        }

        public void handleThrowable(Throwable e) {
            ProtocolProcessException convert = ProtocolProcessException.convert(e);
            transport.out.clear();
            ERR err = new ERR();
            err.sequenceId = 1;
            err.errorCode = convert.getErrorCode();
            err.errorMessage = convert.getMessage();
            transport.out.writeBytes(err.toPacket());
        }

    }

}
