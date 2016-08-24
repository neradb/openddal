package com.openddal.server.util;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.core.ServerSession;

public final class AccessLogger {
    
    private static final Logger accessLogger = LoggerFactory.getLogger("AccessLogger");
    
    
    private static ThreadLocal<TraceData> holder = new ThreadLocal<TraceData>() {
        @Override
        protected TraceData initialValue() {
            return new TraceData();
        }
    };
    
    
    public AccessLogger begin(ServerSession s) {
        if (accessLogger.isInfoEnabled()) {
            TraceData data = holder.get();
            data.s = s;
        }
        return this;
    }
    
    
    
    public AccessLogger seqId(long seqId) {
        if (accessLogger.isInfoEnabled()) {
            TraceData data = holder.get();
            data.seqId = seqId;
        }
        return this;
    }
    
    public AccessLogger command(String command) {
        if (accessLogger.isInfoEnabled()) {
            TraceData data = holder.get();
            data.command = command;
        }
        return this;
    }
    
    public void markError(int errorCode, String errorMsg) {
        if (accessLogger.isInfoEnabled()) {
            TraceData data = holder.get();
            data.errorCode = errorCode;
            data.errorMsg = errorMsg;
        }
    }
    
    public void log() {
        if (accessLogger.isInfoEnabled()) {
            try {
                TraceData data = holder.get().end();
                StringBuilder logMsg = new StringBuilder(256).append("conId:").append(data.s.getThreadId()).append(" ")
                        .append(" seqId:").append(data.seqId).append(" ").append(data.s.getAttachment("remoteAddress"))
                        .append(" ").append(data.s.getAttachment("localAddress")).append(" ").append("command:")
                        .append(data.command).append(" ").append(data.costTime()).append(" ms");
                if (data.errorMsg != null) {
                    logMsg.append(" ErrorCode: ").append(data.errorCode).append(". ").append(data.errorMsg);
                }
                accessLogger.info(logMsg.toString());
            } finally {
                holder.remove();
            }
        }
    }
    


    
    
    public static class TraceData implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long start;
        private long end;
        private long seqId;
        private String command;
        private int errorCode;
        private String errorMsg;
        private ServerSession s;

        public TraceData() {
            start = System.currentTimeMillis();
        }

        public TraceData end() {
            end = System.currentTimeMillis();
            return this;
        }
        
        public long costTime() {
            if (end == 0) {
                throw new IllegalStateException("no end");
            }
            return end - start;
        }
    }


}
