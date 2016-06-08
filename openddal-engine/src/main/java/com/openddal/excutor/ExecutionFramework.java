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
import com.openddal.excutor.handle.HandlerTraceProxy;
import com.openddal.excutor.handle.QueryHandlerFactory;
import com.openddal.excutor.handle.ReadWriteHandler;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.RoutingHandler;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class ExecutionFramework implements PreparedExecutor {

    protected final Session session;
    protected final Database database;
    protected final ThreadPoolExecutor queryExecutor;
    protected final RoutingHandler routingHandler;
    protected final HandlerTraceProxy traceProxy;

    private boolean isPrepared;
    /**
     * @param prepared
     */
    public ExecutionFramework(Session session) {
        this.session = session;
        this.database = session.getDatabase();
        this.queryExecutor = database.getQueryExecutor();
        this.routingHandler = database.getRoutingHandler();
        this.traceProxy = new HandlerTraceProxy(database.getRepository().getQueryHandlerFactory());

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
        checkPrepared();
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
        checkPrepared();
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }
    
    /**
     * Get the PreparedExecutor with the execution explain.
     *
     * @return the execution explain
     */
    public String explain() {
        checkPrepared();
        List<ReadWriteHandler> handlers = traceProxy.getCreatedHandlers();
        if(handlers.size() == 1) {
            return handlers.iterator().next().explain();
        }
        StringBuilder explain = new StringBuilder();
        if(isQuery()) {
            explain.append("MERGE_RESULT");
        } else {
            explain.append("MULTINODES_EXECUTION");
        }
        explain.append('\n');
        for (ReadWriteHandler handler : handlers) {
            String subexplain = handler.explain();
            explain.append(StringUtils.indent(subexplain, 4, false));
        }
        return explain.toString();
    }
    
    @Override
    public boolean isQuery() {
        return false;
    }

    public abstract void doPrepare();

    
    

    public QueryHandlerFactory getQueryHandlerFactory() {
        return traceProxy;
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
