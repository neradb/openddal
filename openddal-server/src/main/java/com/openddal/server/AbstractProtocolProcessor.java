package com.openddal.server;

import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class AbstractProtocolProcessor implements ProtocolProcessor {

    private static final Logger accessLogger = LoggerFactory.getLogger("AccessLogger");
    private static ThreadLocal<ProtocolTransport> transportHolder = new ThreadLocal<ProtocolTransport>();
    private static ThreadLocal<Session> sessionHolder = new ThreadLocal<Session>();
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    @Override
    public final boolean process(ProtocolTransport transport) throws ProtocolProcessException {
        long costTime, start = System.currentTimeMillis();
        ProtocolProcessException e = null;
        try {
            transportHolder.set(transport);
            sessionHolder.set(transport.getSession());
            connHolder.set(transport.getSession().getEngineConnection());
            accessStart(transport, start);
            doProcess(transport);
        } catch (Exception ex) {
            e = ProtocolProcessException.convert(ex);
            throw e;
        } finally {
            costTime = System.currentTimeMillis() - start;
            accessEndLog(transport, costTime, e);
            sessionHolder.remove();
            transportHolder.remove();
            connHolder.remove();
        }
        return e == null;
       
    }

    private void accessStart(ProtocolTransport transport, long processTime) {
        if (accessLogger.isInfoEnabled()) {
            Session session = getSession();
            StringBuilder logMsg = new StringBuilder(256)
                    .append("request-start:{")
                    .append("remote: ").append(transport.getRemoteAddress()).append(", ")
                    .append("local ").append(transport.getLocalAddress()).append(", ")
                    .append("sessionId:").append(session.getSessionId()).append(", ")
                    .append("processTime:").append(processTime).append("")
                    .append("}");
            accessLogger.info(logMsg.toString());

        }
    }

    private void accessEndLog(ProtocolTransport transport, long costTime, ProtocolProcessException e) {
        if (accessLogger.isInfoEnabled()) {
            Session session = getSession();
            StringBuilder logMsg = new StringBuilder(256)
                    .append("request-end:{")
                    .append("remote: ").append(transport.getRemoteAddress()).append(", ")
                    .append("local ").append(transport.getLocalAddress()).append(", ")
                    .append("sessionId:").append(session.getSessionId()).append(", ")
                    .append("costTime:").append(costTime).append("ms, ")
                    .append("status:").append(e == null ? "success" : "failure, ");
            if(e != null) {
                logMsg.append("errorCode:").append(e.getErrorCode()).append(", ")
                .append("errorMsg:").append(e.getErrorMessage());
            }
            logMsg.append("}");
            accessLogger.info(logMsg.toString());

        }
    }
    
    
    protected final Session getSession() {
        return sessionHolder.get();
    }
    protected final Connection getConnection() {
        return connHolder.get();
    }
    
    protected final ProtocolTransport getProtocolTransport() {
        return transportHolder.get();
    }
    

    protected abstract void doProcess(ProtocolTransport transport) throws ProtocolProcessException;

}
