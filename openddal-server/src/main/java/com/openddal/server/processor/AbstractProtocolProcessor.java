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

    @Override
    public void process(Request request, Response response) throws ProtocolProcessException {
        if (accessLogger.isInfoEnabled()) {
            StringBuilder logMsg = new StringBuilder("[ServerStart][sessionId:")
                    .append(request.getSession().getSessionId()).append(" ").append(request.getRemoteAddress())
                    .append(" ").append(request.getLocalAddress()).append(" ").append("packet ").append(".").append(" ")
                    .append(request.getInputBuffer().writerIndex()).append("]");
            accessLogger.info(logMsg.toString());

        }
        switch (request.getSession().getState()) {
        case CONNECTIONING:
            break;
        case CONNECTIONED:
            doProcess(request, response);
            break;

        }

        if (accessLogger.isInfoEnabled()) {
            StringBuilder logMsg = new StringBuilder("[ServerStart][sessionId:")
                    .append(request.getSession().getSessionId()).append(" ").append(request.getRemoteAddress())
                    .append(" ").append(request.getLocalAddress());
            accessLogger.info(logMsg.toString());

        }

    }

    protected abstract void doProcess(Request request, Response response) throws ProtocolProcessException;

}
