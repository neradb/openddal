/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server;

import java.io.Serializable;
import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class TraceableProcessor implements ProtocolProcessor {

    private static final Logger accessLogger = LoggerFactory.getLogger("AccessLogger");

    private static ThreadLocal<ProtocolTransport> transportHolder = new ThreadLocal<ProtocolTransport>();
    private static ThreadLocal<Session> sessionHolder = new ThreadLocal<Session>();
    private static ThreadLocal<Connection> connHolder = new ThreadLocal<Connection>();
    private static ThreadLocal<TraceableData> tdHolder = new ThreadLocal<TraceableData>();

    @Override
    public final boolean process(ProtocolTransport transport) throws ProtocolProcessException {
        ProtocolProcessException e = null;
        try {
            tdHolder.set(new TraceableData());
            transportHolder.set(transport);
            sessionHolder.set(transport.getSession());
            connHolder.set(transport.getSession().getEngineConnection());
            doProcess(transport);
        } catch (Exception ex) {
            e = ProtocolProcessException.convert(ex);
            getTrace().errorCode(e.errorCode).errorMsg(e.getMessage());
            throw e;
        } finally {
            accessEndLog();
            sessionHolder.remove();
            transportHolder.remove();
            connHolder.remove();
            tdHolder.remove();
        }
        return e == null;

    }

    protected abstract void doProcess(ProtocolTransport transport) throws Exception;

    public final TraceableData getTrace() {
        return tdHolder.get();
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

    private void accessEndLog() {
        if (accessLogger.isInfoEnabled()) {
            TraceableData data = getTrace().end();
            Session s = getSession();
            StringBuilder logMsg = new StringBuilder(256).append(data.protocol).append(" ").append(data.sql)
                    .append(" conId:").append(getSession().getConnectionId()).append(" ")
                    .append(s.getAttachment("remoteAddress")).append(" ").append(s.getAttachment("localAddress"))
                    .append(" ").append(data.costTime()).append(" ms");
            if (data.errorMsg != null) {
                logMsg.append(" ErrorCode: ").append(data.errorCode).append(". ").append(data.errorMsg);
            }
            accessLogger.info(logMsg.toString());
        }
    }

    public static class TraceableData implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long start;
        private long end;
        private String sql;
        private String protocol;
        private int errorCode;
        private String errorMsg;

        public TraceableData() {
            start = System.currentTimeMillis();
        }

        public TraceableData end() {
            end = System.currentTimeMillis();
            return this;
        }

        public TraceableData protocol(String protocol) {
            this.protocol = protocol;
            return this;
        }

        public TraceableData sql(String sql) {
            this.sql = sql;
            return this;
        }

        public TraceableData errorCode(int errorCode) {
            this.errorCode = errorCode;
            return this;
        }

        public TraceableData errorMsg(String errorMsg) {
            this.errorMsg = errorMsg;
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
