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

import com.openddal.command.Prepared;
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
import com.openddal.excutor.cursor.Cursor;
import com.openddal.excutor.cursor.MergedCursor;
import com.openddal.excutor.works.BatchUpdateWorker;
import com.openddal.excutor.works.QueryWorker;
import com.openddal.excutor.works.UpdateWorker;
import com.openddal.excutor.works.Worker;
import com.openddal.excutor.works.WorkerFactory;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.RoutingHandler;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class ExecutionFramework<T extends Prepared> implements Executor {

    protected final Session session;
    protected final T prepared;
    protected final Database database;
    protected final ThreadPoolExecutor queryExecutor;
    protected final RoutingHandler routingHandler;
    protected final WorkerFactory queryHandlerFactory;

    private boolean isPrepared;
    private List<Parameter> lastParams;

    /**
     * @param prepared
     */
    public ExecutionFramework(T prepared) {
        this.prepared = prepared;
        this.session = prepared.getSession();
        this.database = session.getDatabase();
        this.queryExecutor = database.getQueryExecutor();
        this.routingHandler = database.getRoutingHandler();
        this.queryHandlerFactory = session.getQueryHandlerFactory();

    }

    @Override
    public final void prepare() {
        List<Parameter> params = prepared.getParameters();
        if (isPrepared && sameParamsAsLast(params, lastParams)) {
            return;
        }
        doPrepare();
        lastParams = params;
        isPrepared = true;
    }

    @Override
    public final int update() {
        prepare();
        return doUpdate();
    }

    @Override
    public final void query() {
        prepare();
        doQuery();
    }

    @Override
    public final String explain() {
        prepare();
        return doExplain();
    }

    protected abstract void doPrepare();

    protected abstract String doExplain();
    
    protected int doUpdate() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    protected void doQuery() {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    protected int invokeUpdateWorker(List<UpdateWorker> handlers) {
        session.checkCanceled();
        try {
            int queryTimeout = session.getQueryTimeout();// MILLISECONDS
            List<Future<Integer>> invokeAll = queryExecutor.invokeAll(handlers, queryTimeout, TimeUnit.MILLISECONDS);
            int affectRows = 0;
            for (Future<Integer> future : invokeAll) {
                affectRows += future.get();
            }
            return affectRows;
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } catch (ExecutionException e) {
            throw DbException.convert(e.getCause());
        } finally {
            session.checkCanceled();
        }
    }

    protected int invokeBatchUpdateWorker(List<BatchUpdateWorker> handlers) {
        session.checkCanceled();
        try {
            int queryTimeout = session.getQueryTimeout();// MILLISECONDS
            List<Future<Integer[]>> invokeAll = queryExecutor.invokeAll(handlers, queryTimeout, TimeUnit.MILLISECONDS);
            int affectRows = 0;
            for (Future<Integer[]> future : invokeAll) {
                Integer[] integers = future.get();
                for (Integer integer : integers) {
                    affectRows += integer;
                }
            }
            return affectRows;
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } catch (ExecutionException e) {
            throw DbException.convert(e.getCause());
        } finally {
            session.checkCanceled();
        }
    }

    protected Cursor invokeQueryWorker(List<QueryWorker> handlers) {
        session.checkCanceled();
        try {
            int queryTimeout = session.getQueryTimeout();// MILLISECONDS
            List<Future<Cursor>> invokeAll = queryExecutor.invokeAll(handlers, queryTimeout, TimeUnit.MILLISECONDS);
            if (invokeAll.size() > 1) {
                MergedCursor cursor = new MergedCursor();
                for (Future<Cursor> future : invokeAll) {
                    cursor.addCursor(future.get());
                }
                return cursor;
            } else {
                return invokeAll.iterator().next().get();
            }
        } catch (InterruptedException e) {
            throw DbException.convert(e);
        } catch (ExecutionException e) {
            throw DbException.convert(e.getCause());
        } finally {
            session.checkCanceled();
        }
    }
    
    protected String explainForUpdateWorker(List<UpdateWorker> workers) {
        if (workers.size() == 1) {
            return workers.iterator().next().explain();
        }
        StringBuilder explain = new StringBuilder();
        explain.append("MULTINODES_EXECUTION");
        explain.append('\n');
        for (Worker worker : workers) {
            String subexplain = worker.explain();
            explain.append(StringUtils.indent(subexplain, 4, false));
        }
        return explain.toString();
    }

    protected TableMate getTableMate(String tableName) {
        TableMate table = findTableMate(tableName);
        if (table != null) {
            return table;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }
    
    protected TableMate toTableMate(Table table) {
        if(table instanceof TableMate) {
            return (TableMate)table;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, table.getName());
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

    protected static TableMate getTableMate(TableFilter filter) {
        if (filter.isFromTableMate()) {
            return (TableMate) filter.getTable();
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, filter.getTable().getName());
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
    
    protected static boolean isConsistencyTableForReferential(TableMate table, TableMate refTable) {
        TableRule t1 = table.getTableRule();
        TableRule t2 = refTable.getTableRule();
        if(t1.getType() != t2.getType()) {
            return false;
        }
        switch (t1.getType()) {
        case TableRule.SHARDED_NODE_TABLE:
            ShardedTableRule shard1 = (ShardedTableRule)t1;
            ShardedTableRule shard2 = (ShardedTableRule)t2;
            return shard1.getOwnerGroup() == shard2.getOwnerGroup();
            
        case TableRule.FIXED_NODE_TABLE:
            String s1 = t1.getMetadataNode().getShardName();
            String s2 = t2.getMetadataNode().getShardName();
            return StringUtils.equals(s1, s2);
        default:
            return false;
        }
        
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
