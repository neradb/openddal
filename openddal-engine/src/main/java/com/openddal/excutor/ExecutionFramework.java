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
package com.openddal.excutor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.openddal.config.TableRule;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.excutor.handle.QueryHandlerFactory;
import com.openddal.excutor.handle.ReadWriteHandler;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.RoutingHandler;
import com.openddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class ExecutionFramework implements PreparedExecutor {

    protected final Session session;
    protected final Database database;
    protected final ThreadPoolExecutor queryExecutor;
    protected final RoutingHandler routingHandler;
    protected final QueryHandlerFactory queryHandlerFactory;

    private boolean isPrepared;
    private List<ReadWriteHandler> handlers = New.arrayList();

    /**
     * @param prepared
     */
    public ExecutionFramework(Session session) {
        this.session = session;
        this.database = session.getDatabase();
        this.queryExecutor = database.getQueryExecutor();
        this.routingHandler = database.getRoutingHandler();
        this.queryHandlerFactory = database.getRepository().getQueryHandlerFactory();

    }

    public void checkPrepared() {
        if (!isPrepared) {
            DbException.throwInternalError("Executor not prepared.");
        }
    }

    public final void prepare() {
        if (isPrepared) {
            return;
        }
        doPrepare();
        isPrepared = true;
    }

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    public int update() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    /**
     * Execute the query.
     *
     * @param maxrows the maximum number of rows to return
     * @return the result set
     * @throws DbException if it is not a query
     */
    public void query() {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    public abstract void doPrepare();

    public void cancel() {
        for (ReadWriteHandler op : handlers) {
            try {
                op.cancel();
            } catch (Throwable e) {

            }
        }
    }

    public static TableMate getTableMate(TableFilter filter) {
        if (filter.isFromTableMate()) {
            return (TableMate) filter.getTable();
        }
        return null;
    }

    public static TableRule getTableRule(TableFilter filter) throws NullPointerException {
        TableMate tableMate = getTableMate(filter);
        TableRule tableRule = tableMate.getTableRule();
        return tableRule;
    }

    public static ArrayList<TableFilter> filterNotTableMate(List<TableFilter> filters) {
        ArrayList<TableFilter> result = New.arrayList(filters.size());
        for (TableFilter tf : result) {
            if (tf.isFromTableMate()) {
                result.add(tf);
            }
        }
        return result;
    }

}
