package com.openddal.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class AbstractProtocolProcessor implements ProtocolProcessor {

    private static final Logger accessLogger = LoggerFactory.getLogger("AccessLogger");
    private static ThreadLocal<Request> requestHolder = new ThreadLocal<Request>();
    private static ThreadLocal<Response> responseHolder = new ThreadLocal<Response>();
    private static ThreadLocal<Session> sessionHolder = new ThreadLocal<Session>();

    @Override
    public final void process(Request request, Response response) throws ProtocolProcessException {
        long costTime, start = System.currentTimeMillis();
        ProtocolProcessException e = null;
        try {
            requestHolder.set(request);
            responseHolder.set(response);
            sessionHolder.set(request.getSession());
            accessStart(start);
            doProcess(request, response);
        } catch (Exception ex) {
            e = ProtocolProcessException.convert(ex);
            throw e;
        } finally {
            costTime = System.currentTimeMillis() - start;
            accessEndLog(costTime, e);
            requestHolder.remove();
            responseHolder.remove();
            sessionHolder.remove();
        }
       
    }

    private void accessStart(long processTime) {
        if (accessLogger.isInfoEnabled()) {
            Request request = getRequest();
            Session session = getSession();
            StringBuilder logMsg = new StringBuilder("request-start:{")
                    .append("remote: ").append(request.getRemoteAddress()).append(", ")
                    .append("local ").append(request.getLocalAddress()).append(", ")
                    .append("sessionId:").append(session.getSessionId()).append(", ")
                    .append("sessionState:").append(session.getState()).append(", ")
                    .append("processor:").append(getClass().getName()).append(", ") 
                    .append("processTime:").append(processTime).append("")
                    .append("}");
            accessLogger.info(logMsg.toString());

        }
    }

    private void accessEndLog(long costTime, ProtocolProcessException e) {
        if (accessLogger.isInfoEnabled()) {
            Request request = getRequest();
            Session session = getSession();
            StringBuilder logMsg = new StringBuilder("request-end:{")
                    .append("remote: ").append(request.getRemoteAddress()).append(", ")
                    .append("local ").append(request.getLocalAddress()).append(", ")
                    .append("sessionId:").append(session.getSessionId()).append(", ")
                    .append("sessionState:").append(session.getState()).append(", ")
                    .append("processor:").append(getClass().getName()).append(", ") 
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
    
    protected final Request getRequest() {
        return requestHolder.get();
    }
    
    protected final Response getResponse() {
        return responseHolder.get();
    }
    
    protected final Session getSession() {
        return sessionHolder.get();
    }
    

    protected abstract void doProcess(Request request, Response response) throws ProtocolProcessException;

}
