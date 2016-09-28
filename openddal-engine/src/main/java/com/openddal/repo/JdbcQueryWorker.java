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
package com.openddal.repo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.openddal.engine.Session;
import com.openddal.executor.cursor.Cursor;
import com.openddal.executor.cursor.ResultCursor;
import com.openddal.executor.works.QueryWorker;
import com.openddal.util.JdbcUtils;
import com.openddal.util.StatementBuilder;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 *
 */
public class JdbcQueryWorker extends JdbcWorker implements QueryWorker {

    private Connection conn;
    private PreparedStatement stmt;
    private ResultSet set;
    
    public JdbcQueryWorker(Session session, String shardName, String sql, List<Value> params) {
        super(session, shardName, sql, params);
    }

    @Override
    public Cursor call() throws Exception {
        return executeQuery();
    }

    @Override
    public Cursor executeQuery() {
        try {
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {1};", shardName, sql);
            }
            conn = borrowConnection();
            stmt = conn.prepareStatement(sql);
            applyQueryTimeout(stmt);
            if (params != null) {
                for (int i = 0, size = params.size(); i < size; i++) {
                    Value v = params.get(i);
                    v.set(stmt, i + 1);
                    if (trace.isDebugEnabled()) {
                        trace.debug("{0} setParameter: {1} -> {2};", shardName, i + 1, v.getSQL());
                    }
                }
            }
            set = stmt.executeQuery();
            return new ResultCursor(session, set);
        } catch (SQLException e) {
            close();
            StatementBuilder buff = new StatementBuilder();
            buff.append(sql);
            if (params != null && params.size() > 0) {
                buff.append(" params{");
                int i = 1;
                for (Value v : params) {
                    buff.appendExceptFirst(", ");
                    buff.append(i++).append(": ").append(v.getSQL());
                }
                buff.append('}');
            }
            throw wrapException("executeQuery", shardName, buff.toString(), e);
        } finally {
            //see Session.endStatement
        }

    }

    public void cancel() {
        try {
            if (stmt == null) {
                stmt.cancel();
            }
        } catch (Exception e) {
            trace.error(e, "cancel worker error.");
        }
    }

    public void close() {
        JdbcUtils.closeSilently(set);
        JdbcUtils.closeSilently(stmt);
        returnConnection(conn);
        set = null;
        stmt = null;
        conn = null;
    }


}
