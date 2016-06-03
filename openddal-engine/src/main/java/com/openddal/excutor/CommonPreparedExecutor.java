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
// Created on 2015年4月12日
// $Id$

package com.openddal.excutor;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.openddal.command.Prepared;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.LocalResult;
import com.openddal.result.ResultTarget;
import com.openddal.route.RoutingHandler;
import com.openddal.util.New;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public abstract class CommonPreparedExecutor<T extends Prepared> implements PreparedExecutor {

    protected final T prepared;
    protected final Session session;
    protected final Database database;
    protected final ThreadPoolExecutor jdbcExecutor;
    protected final List<JdbcWorker<?>> runingWorkers;
    protected final RoutingHandler routingHandler;


    /**
     * @param prepared
     */
    public CommonPreparedExecutor(T prepared) {
        super();
        this.prepared = prepared;
        this.session = prepared.getSession();
        this.database = session.getDatabase();
        this.jdbcExecutor = session.getDatabase().getQueryExecutor();
        this.runingWorkers = New.linkedList();
        this.routingHandler = database.getRoutingHandler();

    }

    /**
     * Execute the query.
     *
     * @param maxRows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    @Override
    public LocalResult executeQuery(int maxRows) {
        return executeQuery(maxRows, null);
    }

    /**
     * Execute the query, writing the result to the target result.
     *
     * @param maxRows the maximum number of rows to return
     * @param target  the target result (null will return the result)
     * @return the result set (if the target is not set).
     */
    @Override
    public LocalResult executeQuery(int maxRows, ResultTarget target) {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    @Override
    public int executeUpdate() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    protected T getPrepared() {
        return prepared;
    }

    /**
     * Gets the current query timeout in MILLISECONDS.
     *
     * @return query timeout
     * @see Session.getQueryTimeout()
     */
    protected int getQueryTimeout() {
        return session.getQueryTimeout();
    }

    /**
     * @param tableName
     */
    public TableMate getTableMate(String tableName) {
        TableMate table = findTableMate(tableName);
        if (table != null) {
            return table;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

    /**
     * @param tableName
     */
    public TableMate findTableMate(String tableName) {
        Table table = database.getSchema(session.getCurrentSchemaName()).findTableOrView(session, tableName);
        if (table == null) {
            String[] schemaNames = session.getSchemaSearchPath();
            if (schemaNames != null) {
                for (String name : schemaNames) {
                    Schema s = database.getSchema(name);
                    table = s.findTableOrView(session, tableName);
                    if (table != null) {
                        break;
                    }
                }
            }
        }
        if (table != null && table instanceof TableMate) {
            return (TableMate) table;
        }
        return null;
    }

    @Override
    public void kill() {
        for (JdbcWorker<?> jdbcWorker : runingWorkers) {
            jdbcWorker.cancel();
            jdbcWorker.closeResource();
        }
        runingWorkers.clear();
    }

    protected <E> void addRuningJdbcWorker(JdbcWorker<E> worker) {
        runingWorkers.add(worker);
    }

    protected <E> void addRuningJdbcWorkers(List<JdbcWorker<E>> workers) {
        runingWorkers.addAll(workers);
    }

    protected <E> void removeRuningJdbcWorker(JdbcWorker<E> worker) {
        runingWorkers.remove(worker);
    }

    protected <E> void removeRuningJdbcWorkers(List<JdbcWorker<E>> workers) {
        runingWorkers.removeAll(workers);
    }

    protected TableMate castTableMate(Table table) {
        if (table instanceof TableMate) {
            return (TableMate) table;
        }
        String className = table == null ? "null" : table.getClass().getName();
        throw new IllegalStateException("Type mismatch:" + className);
    }

    protected JdbcWorker<Integer> createUpdateWorker(String shardName, String sql, List<Value> params) {
        return new JdbcUpdateWorker(session, shardName, sql, params);
    }

    protected JdbcWorker<ResultSet> createQueryWorker(String shardName, String sql, List<Value> params, int maxrows) {
        return new JdbcQueryWorker(session, shardName, sql, params, maxrows);
    }

    protected JdbcWorker<Integer[]> createBatchUpdateWorker(String shardName, String sql, List<List<Value>> array) {
        return new JdbcBatchUpdateWorker(session, shardName, sql, array);
    }

    protected String identifier(String s) {
        return database.identifier(s);
    }
}
