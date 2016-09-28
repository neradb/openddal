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
package com.openddal.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.openddal.config.GlobalTableRule;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.executor.cursor.Cursor;
import com.openddal.executor.cursor.MergedCursor;
import com.openddal.executor.works.QueryWorker;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.executor.works.Worker;
import com.openddal.executor.works.WorkerFactory;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.Row;
import com.openddal.route.RoutingHandler;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

/**
 * @author jorgie.li
 *
 */
public abstract class ExecutionFramework implements Executor {

    protected Session session;
    protected Database database;
    protected ThreadPoolExecutor queryExecutor;
    protected RoutingHandler routingHandler;
    protected WorkerFactory queryHandlerFactory;

    private boolean isPrepared;

    protected final void prepare(Session s) {
        if (isPrepared) {
            return;
        }
        this.session = s;
        this.database = session.getDatabase();
        this.queryExecutor = database.getQueryExecutor();
        this.routingHandler = database.getRoutingHandler();
        this.queryHandlerFactory = session.getQueryHandlerFactory();
        doPrepare();
        isPrepared = true;
    }

    @Override
    public final int update(Session s) {
        prepare(s);
        return doUpdate();
    }

    @Override
    public final Cursor query(Session s) {
        prepare(s);
        return doQuery();
    }

    @Override
    public final String explain(Session s) {
        prepare(s);
        return doExplain();
    }

    protected abstract void doPrepare();

    protected abstract String doExplain();

    protected int doUpdate() {
        throw DbException.get(ErrorCode.METHOD_NOT_ALLOWED_FOR_QUERY);
    }

    protected Cursor doQuery() {
        throw DbException.get(ErrorCode.METHOD_ONLY_ALLOWED_FOR_QUERY);
    }

    protected int invokeUpdateWorker(List<UpdateWorker> worker) {
        session.checkCanceled();
        try {
            int queryTimeout = session.getQueryTimeout();// MILLISECONDS
            List<Future<Integer>> invokeAll;
            if (queryTimeout > 0) {
                invokeAll = queryExecutor.invokeAll(worker, queryTimeout, TimeUnit.MILLISECONDS);
            } else {
                invokeAll = queryExecutor.invokeAll(worker);
            }
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

    protected Cursor invokeQueryWorker(List<QueryWorker> worker) {
        session.checkCanceled();
        try {
            int queryTimeout = session.getQueryTimeout();// MILLISECONDS
            List<Future<Cursor>> invokeAll;
            if (queryTimeout > 0) {
                invokeAll = queryExecutor.invokeAll(worker, queryTimeout, TimeUnit.MILLISECONDS);
            } else {
                invokeAll = queryExecutor.invokeAll(worker);
            }
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

    protected String explainForWorker(List<? extends Worker> workers) {
        StringBuilder explain = new StringBuilder();
        if (workers.size() == 1) {
            explain.append("SINGLE_EXECUTION");
            explain.append('\n');
            explain.append(StringUtils.indent(workers.iterator().next().explain(), 4, false));
        } else {
            explain.append("MULTIPLE_EXECUTION");
            for (Worker worker : workers) {
                String subexplain = worker.explain();
                explain.append('\n');
                explain.append(StringUtils.indent(subexplain, 4, false));
            }
        }
        return explain.toString();
    }
    
    

    protected boolean isPrepared() {
        return isPrepared;
    }

    protected Map<ObjectNode, List<Row>> batchForRoutingNode(TableMate table, List<Row> rows) {
        Map<ObjectNode, List<Row>> batches = New.hashMap();
        for (Row row : rows) {
            RoutingResult result;
            if (table.getTableRule().getType() == TableRule.GLOBAL_NODE_TABLE) {
                GlobalTableRule rule = (GlobalTableRule) table.getTableRule();
                result = rule.getBroadcastsRoutingResult();
            } else {
                result = routingHandler.doRoute(table, row);
            }
            ObjectNode[] selectNodes = result.getSelectNodes();
            for (ObjectNode objectNode : selectNodes) {
                List<Row> batch = batches.get(objectNode);
                if (batch == null) {
                    batch = New.arrayList(10);
                    batches.put(objectNode, batch);
                }
                batch.add(row);
            }
        }
        return batches;
    }

    protected TableMate getTableMate(String tableName) {
        TableMate table = findTableMate(tableName);
        if (table != null) {
            return table;
        }
        throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
    }

    protected TableMate toTableMate(Table table) {
        if (table instanceof TableMate) {
            return (TableMate) table;
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

    protected static ArrayList<TableFilter> filterNotTableMate(TableFilter f) {
        ArrayList<TableFilter> result = New.arrayList();
        do {
            if (f.isFromTableMate()) {
                result.add(f);
            }
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                result.addAll(filterNotTableMate(n));
            }
            f = f.getJoin();
        } while (f != null);
        return result;
    }

    protected static boolean isConsistencyNodeForReferential(TableMate table, TableMate refTable) {
        TableRule t1 = table.getTableRule();
        TableRule t2 = refTable.getTableRule();
        if (t1.getType() != t2.getType()) {
            return false;
        }
        switch (t1.getType()) {
        case TableRule.SHARDED_NODE_TABLE:
            ShardedTableRule shard1 = (ShardedTableRule) t1;
            ShardedTableRule shard2 = (ShardedTableRule) t2;
            return shard1.getOwnerGroup() == shard2.getOwnerGroup();

        case TableRule.GLOBAL_NODE_TABLE:
            GlobalTableRule g1 = (GlobalTableRule) t1;
            GlobalTableRule g2 = (GlobalTableRule) t2;
            ObjectNode[] ns1 = g1.getBroadcasts();
            ObjectNode[] ns2 = g2.getBroadcasts();
            Set<String> set1 = New.hashSet();
            Set<String> set2 = New.hashSet();
            for (ObjectNode n1 : ns1) {
                set1.add(n1.getShardName());
            }
            for (ObjectNode n2 : ns2) {
                set2.add(n2.getShardName());
            }
            return set1.equals(set2);

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
