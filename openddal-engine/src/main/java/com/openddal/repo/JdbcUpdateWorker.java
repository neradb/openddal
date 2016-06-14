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

import java.util.List;

import com.openddal.engine.Session;
import com.openddal.excutor.works.UpdateWorker;
import com.openddal.util.StatementBuilder;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class JdbcUpdateWorker extends JdbcWorker implements UpdateWorker {

    public JdbcUpdateWorker(Session session, String shardName, String sql, List<Value> params) {
        super(session, shardName, sql, params);
    }

    @Override
    public Integer call() throws Exception {
        return executeUpdate();
    }

    @Override
    public int executeUpdate() {
        try {
            Navigator optional = Navigator.build().shardName(shardName).readOnly(true);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Fetching connection from DataSource.", shardName);
            }
            opendConnection = doGetConnection(optional);
            if (trace.isDebugEnabled()) {
                trace.debug("{0} Preparing: {1};", shardName, sql);
            }
            opendStatement = opendConnection.prepareStatement(sql);
            applyQueryTimeout(opendStatement);
            if (params != null) {
                for (int i = 0, size = params.size(); i < size; i++) {
                    Value v = params.get(i);
                    v.set(opendStatement, i + 1);
                    if (trace.isDebugEnabled()) {
                        trace.debug("{0} setParameter: {1} -> {2};", shardName, i + 1, v.getSQL());
                    }
                }
            }
            int rows = opendStatement.executeUpdate();
            if (trace.isDebugEnabled()) {
                trace.debug("{0} executeUpdate: {1} affected.", shardName, rows);
            }
            return rows;
        } catch (Exception e) {
            StatementBuilder buff = new StatementBuilder();
            buff.append(shardName).append(" executing executeUpdate error:").append(sql);
            if (params != null && 0 < params.size()) {
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
            close();
        }
    }
}
