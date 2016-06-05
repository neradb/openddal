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
package com.openddal.repo.handle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import com.openddal.engine.Session;
import com.openddal.excutor.cursor.Cursor;
import com.openddal.excutor.cursor.ResultCursor;
import com.openddal.excutor.handle.QueryHandler;
import com.openddal.repo.ShardSelector;
import com.openddal.util.JdbcUtils;
import com.openddal.util.StatementBuilder;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class JdbcUpdateHandler extends JdbcBasicHandler implements QueryHandler {

    public JdbcUpdateHandler(Session session, String shardName, String sql, List<Value> params) {
        super(session, shardName, sql, params);
    }

    @Override
    public Cursor call() throws Exception {
        return executeQuery();
    }

    @Override
    public Cursor executeQuery() {
        Connection conn = null;
        PreparedStatement stmt = null;
        try {
            DataSource dataSource = getDataSource();
            ShardSelector optional = ShardSelector.build().shardName(shardName).readOnly(true);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Fetching connection from DataSource.", shardName);
            }
            conn = session.applyConnection(dataSource, optional);
            attach(conn);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {};", shardName, sql);
            }
            stmt = conn.prepareStatement(sql);
            attach(stmt);
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
            ResultSet result = stmt.executeQuery();
            attach(result);
            return new ResultCursor(session, result);
        } catch (SQLException e) {
            StatementBuilder buff = new StatementBuilder();
            buff.append(shardName).append(" executing executeQuery error:").append(sql);
            if (params != null && params.size() > 0) {
                buff.append("\n{");
                int i = 1;
                for (Value v : params) {
                    buff.appendExceptFirst(", ");
                    buff.append(i++).append(": ").append(v.getSQL());
                }
                buff.append('}');
            }
            buff.append(';');
            trace.error(e, buff.toString());
            throw wrapException(sql, e);
        } finally {
            JdbcUtils.closeSilently(conn);
        }

    }
}
