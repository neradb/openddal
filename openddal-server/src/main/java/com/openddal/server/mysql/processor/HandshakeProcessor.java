package com.openddal.server.mysql.processor;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.NettyServer;
import com.openddal.server.mysql.Capabilities;
import com.openddal.server.mysql.Charsets;
import com.openddal.server.mysql.ErrorCode;
import com.openddal.server.mysql.packet.AuthPacket;
import com.openddal.server.mysql.packet.HandshakePacket;
import com.openddal.server.mysql.packet.MySQLPacket;
import com.openddal.server.mysql.packet.QuitPacket;
import com.openddal.server.processor.AbstractProtocolProcessor;
import com.openddal.server.processor.ProtocolProcessException;
import com.openddal.server.processor.Request;
import com.openddal.server.processor.Response;
import com.openddal.server.processor.Session;
import com.openddal.server.processor.Session.State;
import com.openddal.server.processor.SessionImpl;
import com.openddal.server.util.RandomUtil;

import io.netty.buffer.ByteBuf;

public class HandshakeProcessor extends AbstractProtocolProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(HandshakeProcessor.class);

    private static final String AUTH_SEED_KEY = "_AUTH_SEED_KEY";
    private static final String CHARSET_INDEX_KEY = "_CHARSET_INDEX_KEY";
    private static final String DEFAULT_CHARSET = "utf8";
    private final String charset = System.getProperty("ddal.server.charset", DEFAULT_CHARSET);
    private static final byte[] AUTH_OK = new byte[] { 7, 0, 0, 2, 0, 0, 0, 2, 0, 0, 0 };

    @Override
    protected void doProcess(Request request, Response response) throws ProtocolProcessException {
        Session session = request.getSession();
        State state = session.getState();
        switch (state) {
        case NEW:
            writeHandshakePacket();
            break;
        case CONNECTIONING:
            
            byte[] data = new byte[readedLength];
            buf.get(data, start, readedLength);
            // check quit packet
            if (data.length == QuitPacket.QUIT.length
                    && data[4] == MySQLPacket.COM_QUIT) {
                source.close("quit packet");
                return;
            }
            AuthPacket auth = new AuthPacket();
            auth.read(data);

            // check user
            if (!checkUser(source, auth.user, source.getHost())) {
                failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR,
                        "Access denied for user '" + auth.user + "'");
                return;
            }

            // check password
            if (!checkPassword(source, auth.password, auth.user)) {
                failure(source, ErrorCode.ER_ACCESS_DENIED_ERROR,
                        "Access denied for user '" + auth.user + "'");
                return;
            }

            // check schema
            switch (checkSchema(source, auth.database, auth.user)) {
            case ErrorCode.ER_BAD_DB_ERROR:
                failure(source, ErrorCode.ER_BAD_DB_ERROR, "Unknown database '"
                        + auth.database + "'");
                break;
            case ErrorCode.ER_DBACCESS_DENIED_ERROR:
                String s = "Access denied for user '" + auth.user
                        + "' to database '" + auth.database + "'";
                failure(source, ErrorCode.ER_DBACCESS_DENIED_ERROR, s);
                break;
            default:
                success(auth);
            }
        
            break;
        default:
            break;
        }
        

    }
    
    public boolean setCharsetIndex(int ci) {
        String charset = Charsets.getCharset(ci);
        if (charset != null) {
            return setCharset(charset);
        } else {
            return false;
        }
    }
    
    public void writeHandshakePacket() throws IOException {
        Session session = getSession();
        // 生成认证数据
        byte[] rand1 = RandomUtil.randomBytes(8);
        byte[] rand2 = RandomUtil.randomBytes(12);

        // 保存认证数据
        byte[] seed = new byte[rand1.length + rand2.length];
        System.arraycopy(rand1, 0, seed, 0, rand1.length);
        System.arraycopy(rand2, 0, seed, rand1.length, rand2.length);
        session.setAttachment(AUTH_SEED_KEY, seed);
        output.writeBytes(AUTH_OK);
        // 发送握手数据包
        HandshakePacket hs = new HandshakePacket();
        hs.packetId = 0;
        hs.protocolVersion = NettyServer.PROTOCOL_VERSION;
        hs.serverVersion = NettyServer.SERVER_VERSION;
        hs.threadId = session.getSessionId();
        hs.seed = rand1;
        hs.serverCapabilities = getServerCapabilities();
        hs.serverCharsetIndex = (byte) (charsetIndex & 0xff);
        hs.serverStatus = 2;
        hs.restOfScrambleBuff = rand2;
        hs.write(getResponse().getOutputBuffer());
    }

    protected void success(AuthPacket auth) {
        SessionImpl session = (SessionImpl)getSession();
        session.setAuthenticated(true);
        session.setUser(auth.user);
        session.setSchema(auth.database);
        //session.setCharsetIndex(auth.charsetIndex);
        if (LOGGER.isInfoEnabled()) {
            StringBuilder s = new StringBuilder();
            s.append(getRequest().getRemoteAddress()).append('\'').append(auth.user).append("' login success");
            byte[] extra = auth.extra;
            if (extra != null && extra.length > 0) {
                s.append(",extra:").append(new String(extra));
            }
            LOGGER.info(s.toString());
        }
        ByteBuf output = getResponse().getOutputBuffer();
        boolean clientCompress = Capabilities.CLIENT_COMPRESS == (Capabilities.CLIENT_COMPRESS & auth.clientFlags);
        output.writeBytes(AUTH_OK);
        session.setState(State.CONNECTIONED);
    }
    
    protected int getServerCapabilities() {
        int flag = 0;
        flag |= Capabilities.CLIENT_LONG_PASSWORD;
        flag |= Capabilities.CLIENT_FOUND_ROWS;
        flag |= Capabilities.CLIENT_LONG_FLAG;
        flag |= Capabilities.CLIENT_CONNECT_WITH_DB;
        // flag |= Capabilities.CLIENT_NO_SCHEMA;
        // flag |= Capabilities.CLIENT_COMPRESS;
        flag |= Capabilities.CLIENT_ODBC;
        flag |= Capabilities.CLIENT_LOCAL_FILES;
        flag |= Capabilities.CLIENT_IGNORE_SPACE;
        flag |= Capabilities.CLIENT_PROTOCOL_41;
        flag |= Capabilities.CLIENT_INTERACTIVE;
        // flag |= Capabilities.CLIENT_SSL;
        flag |= Capabilities.CLIENT_IGNORE_SIGPIPE;
        flag |= Capabilities.CLIENT_TRANSACTIONS;
        // flag |= ServerDefs.CLIENT_RESERVED;
        flag |= Capabilities.CLIENT_SECURE_CONNECTION;
        return flag;
    }
    

}
