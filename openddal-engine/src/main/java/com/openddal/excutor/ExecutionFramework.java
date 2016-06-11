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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.openddal.command.expression.Parameter;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.excutor.handle.HandlerHolderProxy;
import com.openddal.excutor.handle.QueryHandlerFactory;
import com.openddal.excutor.handle.ReadWriteHandler;
import com.openddal.excutor.handle.UpdateHandler;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.RoutingHandler;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class ExecutionFramework implements Executor {

    protected final Session session;
    protected final Database database;
    protected RoutingResult routingResult = null;
    protected final ThreadPoolExecutor queryExecutor;
    protected final RoutingHandler routingHandler;
    protected final HandlerHolderProxy queryHandlerFactory;

    private boolean isPrepared;
    private List<Parameter> lastParams;


    /**
     * @param prepared
     */
    public ExecutionFramework(Session session) {
        this.session = session;
        this.database = session.getDatabase();
        this.queryExecutor = database.getQueryExecutor();
        this.routingHandler = database.getRoutingHandler();
        this.queryHandlerFactory = new HandlerHolderProxy(session.getQueryHandlerFactory());

    }

    public void checkPrepared() {
        if (!isPrepared) {
            DbException.throwInternalError("Executor not prepared.");
        }
    }

    public final void prepare() {
        List<Parameter> params = getPreparedParameters();
        if (isPrepared && sameParamsAsLast(params, lastParams)) {
            return;
        }
        doPrepare();
        lastParams = params;
        isPrepared = true;
    }

    private boolean sameParamsAsLast(List<Parameter> params, List<Parameter> lastParams) {
        Database db = session.getDatabase();
        params = params == null ? New.<Parameter> arrayList() : params;
        lastParams = lastParams == null ? New.<Parameter> arrayList() : lastParams;
        if (params.size() != lastParams.size()) {
            return false;
        }
        for (int i = 0; i < params.size(); i++) {
            Value a = params.get(i).getParamValue(), b = params.get(i).getParamValue();
            if (a.getType() != b.getType() || !db.areEqual(a, b)) {
                return false;
            }
        }
        return true;
    }

    protected abstract List<Parameter> getPreparedParameters();

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
        if (!queryHandlerFactory.hasCreatedHandlers()) {
            return null;
        }
        List<ReadWriteHandler> handlers = queryHandlerFactory.getCreatedHandlers();
        if (handlers.size() == 1) {
            return handlers.iterator().next().explain();
        }
        StringBuilder explain = new StringBuilder();
        if (isQuery()) {
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


    public abstract void doPrepare();

    public QueryHandlerFactory getQueryHandlerFactory() {
        return queryHandlerFactory;
    }

    protected boolean isQuery() {
        return false;
    }
    public int executeUpdateHandlers(List<UpdateHandler> handlers) {
        session.checkCanceled();
        try {
            int affectRows = 0;
            if (handlers.size() > 1) {
                int queryTimeout = session.getQueryTimeout();// MILLISECONDS
                List<Future<Integer>> invokeAll;
                if (queryTimeout > 0) {
                    invokeAll = queryExecutor.invokeAll(handlers, queryTimeout, TimeUnit.MILLISECONDS);
                } else {
                    invokeAll = queryExecutor.invokeAll(handlers);
                }
                for (Future<Integer> future : invokeAll) {
                    affectRows += future.get();
                }
            } else if (handlers.size() == 1) {
                handlers.iterator().next().executeUpdate();
            }
            return affectRows;
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } catch (ExecutionException e) {
            throw DbException.convert(e.getCause());
        } finally {
            for (UpdateHandler handler : handlers) {
                handler.close();
            }
        }
    }

    protected static TableMate getTableMate(TableFilter filter) {
        if (filter.isFromTableMate()) {
            return (TableMate) filter.getTable();
        }
        return null;
    }

    protected TableMate getTableMate(String tableName) {
        TableMate table = findTableMate(tableName);
        if (table != null) {
            return table;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

    protected TableMate findTableMate(String tableName) {
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

    protected static TableRule getTableRule(TableFilter filter) throws NullPointerException {
        TableMate tableMate = getTableMate(filter);
        TableRule tableRule = tableMate.getTableRule();
        return tableRule;
    }

    protected static ArrayList<TableFilter> filterNotTableMate(List<TableFilter> filters) {
        ArrayList<TableFilter> result = New.arrayList(filters.size());
        for (TableFilter tf : result) {
            if (tf.isFromTableMate()) {
                result.add(tf);
            }
        }
        return result;
    }

    protected static ObjectNode getConsistencyNode(TableRule tableRule, ObjectNode target) {
        switch (tableRule.getType()) {
        case TableRule.SHARDED_NODE_TABLE:
            ShardedTableRule shardTable = (ShardedTableRule) tableRule;
            ObjectNode[] objectNodes = shardTable.getObjectNodes();
            for (ObjectNode objectNode : objectNodes) {
                if (StringUtils.equals(target.getShardName(), objectNode.getShardName())
                        && StringUtils.equals(target.getSuffix(), objectNode.getSuffix())) {
                    return objectNode;
                }
            }
            throw DbException.throwInternalError("The sharding table " + shardTable.getName()
                    + " not have the consistency TableNode for node " + target.toString());
        case TableRule.GLOBAL_NODE_TABLE:
            GlobalTableRule globalTable = (GlobalTableRule) tableRule;
            objectNodes = globalTable.getBroadcasts();
            for (ObjectNode objectNode : objectNodes) {
                if (StringUtils.equals(target.getShardName(), objectNode.getShardName())) {
                    return objectNode;
                }
            }
            throw DbException.throwInternalError("The global table " + globalTable.getName()
                    + " not have the TableNode on shard " + target.getShardName());
        case TableRule.FIXED_NODE_TABLE:
            ObjectNode objectNode = tableRule.getMetadataNode();
            if (StringUtils.equals(target.getShardName(), objectNode.getShardName())) {
                return objectNode;
            }
            throw DbException.throwInternalError(
                    "The table " + tableRule.getName() + " not have the TableNode on shard " + target.getShardName());
        default:
            throw DbException.throwInternalError();
        }
    }

}
