package com.openddal.excutor.cursor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.openddal.command.dml.Select;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Constants;
import com.openddal.engine.Session;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.message.DbException;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

public class DirectLookupCursor extends ExecutionFramework implements Cursor {

    private final Select select;
    private Cursor cursor;
    private Map<ObjectNode,Map<TableFilter,ObjectNode>> consistencyTableNodes;

    public DirectLookupCursor(Session session, Select select) {
        super(session);
        this.select = select;
    }

    @Override
    public void doPrepare() {
        doRoute(select);
        
    }

    private RoutingResult doRoute(Select prepare) {
        List<TableFilter> filters = filterNotTableMate(prepare.getTopFilters());
        List<TableFilter> shards = New.arrayList(filters.size());
        List<TableFilter> globals = New.arrayList(filters.size());
        List<TableFilter> fixeds = New.arrayList(filters.size());
        for (TableFilter tf : filters) {
            TableMate table = getTableMate(tf);
            switch (table.getTableRule().getType()) {
            case TableRule.SHARDED_NODE_TABLE:
                shards.add(tf);
                break;
            case TableRule.GLOBAL_NODE_TABLE:
                globals.add(tf);
                break;
            case TableRule.FIXED_NODE_TABLE:
                fixeds.add(tf);
                break;
            default:
                break;
            }
        }
        RoutingResult result = null;
        if (!shards.isEmpty()) {
            for (TableFilter tf : shards) {
                TableMate table = getTableMate(tf);
                ArrayList<IndexCondition> routeConds = tf.getIndexConditions();
                RoutingResult r = routingHandler.doRoute(session, table, routeConds);
                result = (result == null || r.compareTo(result) < 0) ? r : result;
            }
        } else if (!fixeds.isEmpty()) {
            for (TableFilter tf : shards) {
                TableMate table = getTableMate(tf);
                RoutingResult r = routingHandler.doRoute(table);
                result = r;
            }
        } else if (!globals.isEmpty()) {
            // 全部为全局表查询，取一个
            for (TableFilter tf : shards) {
                TableMate table = getTableMate(tf);
                RoutingResult r = routingHandler.doRoute(table);
                result = r;
            }
        } else {
            throw DbException.throwInternalError("SQL_ROUTING_ERROR");
        }
        ObjectNode[] selectNodes = result.getSelectNodes();
        if (session.getDatabase().getSettings().optimizeMerging) {
            selectNodes = result.group();
        }
        if(selectNodes.length == 0) {
            throw DbException.throwInternalError("SQL_ROUTING_ERROR,empty result");
        }
        setConsistencyTableNodes(selectNodes, filters);
        return result;
    }

    private void setConsistencyTableNodes(ObjectNode[] selectNodes, List<TableFilter> filters) {
        this.consistencyTableNodes = New.hashMapNonRehash(selectNodes.length);
        for (ObjectNode target : selectNodes) {
            HashMap<TableFilter, ObjectNode> tableNodeMapping = New.hashMapNonRehash(filters.size());
            for (TableFilter tf : filters) {
                TableMate table = getTableMate(tf);
                ObjectNode consistencyNode = getConsistencyNode(table.getTableRule(), target);
                tableNodeMapping.put(tf, consistencyNode);
            }
            consistencyTableNodes.put(target, tableNodeMapping);
        }
    }

    private ObjectNode getConsistencyNode(TableRule tableRule, ObjectNode target) {
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
            objectNodes = globalTable.getObjectNodes();
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

    @Override
    public Row get() {
        if (cursor == null) {
            return null;
        }
        return cursor.get();
    }

    @Override
    public SearchRow getSearchRow() {
        return cursor.getSearchRow();
    }

    @Override
    public boolean next() {
        while (true) {
            if (cursor == null) {
                nextCursor();
                if (cursor == null) {
                    return false;
                }
            }
            if (cursor.next()) {
                return true;
            }
            cursor = null;
        }
    }

    private void nextCursor() {

    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }

    public double getCost() {
        // TODO Auto-generated method stub
        return 0;
    }

    public static boolean isDirectLookupQuery(Select select) {
        DirectLookupEstimator estimator = new DirectLookupEstimator(select.getTopFilters());
        return estimator.isDirectLookup();
    }

    private static class DirectLookupEstimator {

        private final ArrayList<TableFilter> filters;
        private Set<TableFilter> joinTableChain;

        private DirectLookupEstimator(ArrayList<TableFilter> topFilters) {
            this.filters = New.arrayList(topFilters.size());
            for (TableFilter tf : topFilters) {
                if (!StringUtils.startsWith(tf.getTableAlias(), Constants.PREFIX_JOIN)) {
                    filters.add(tf);
                }
            }
            for (TableFilter tf : filters) {
                if (tf.getTable() instanceof TableMate) {
                    continue;
                }
            }
        }

        private boolean isDirectLookup() {
            for (TableFilter tf : filters) {
                if (!tf.isFromTableMate()) {
                    return false;
                }
            }
            if (filters.size() == 1) {
                return true;
            } else {
                List<TableFilter> shardingTableFilter = New.arrayList();
                for (TableFilter tf : filters) {
                    if (!isGropTableFilter(tf)) {
                        return false;
                    }
                    TableMate table = (TableMate) tf.getTable();
                    if (table.getRuleColumns() != null) {
                        shardingTableFilter.add(tf);
                    }
                }
                if (shardingTableFilter.size() < 2) {
                    return true;
                }
                this.joinTableChain = New.hashSet();
                evaluationJoinChain(shardingTableFilter.iterator().next());
                return joinTableChain.containsAll(shardingTableFilter);
            }

        }

        private boolean isGropTableFilter(TableFilter filter) {
            TableMate table1 = (TableMate) filter.getTable();
            for (TableFilter item : filters) {
                if (item == filter) {
                    continue;
                }
                TableMate table2 = (TableMate) item.getTable();
                TableRule rule1 = table1.getTableRule();
                TableRule rule2 = table2.getTableRule();
                if (!rule1.isNodeComparable(rule2)) {
                    return false;
                }
            }
            return true;
        }

        private void evaluationJoinChain(TableFilter filter) {
            TableMate table1 = (TableMate) filter.getTable();
            Column[] columns1 = table1.getRuleColumns();
            if (columns1 == null) {
                throw new IllegalArgumentException("not sharding TableFilter");
            }
            ArrayList<IndexCondition> conditions = filter.getIndexConditions();
            int length = table1.getColumns().length;
            IndexCondition[] masks = new IndexCondition[length];
            for (IndexCondition condition : conditions) {
                int id = condition.getColumn().getColumnId();
                if (id >= 0) {
                    masks[id] = condition;
                }
            }

            if (masks != null) {
                List<Column> joinCols = New.arrayList();
                for (int i = 0, len = columns1.length; i < len; i++) {
                    Column column = columns1[i];
                    int index = column.getColumnId();
                    IndexCondition mask = masks[index];
                    if ((mask.getMask(conditions) & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                        Column compareColumn = mask.getCompareColumn();
                        if (compareColumn != null) {
                            joinCols.add(compareColumn);
                        }
                        if (i == columns1.length - 1) {
                            Set<Table> tables = New.hashSet();
                            for (Column column2 : joinCols) {
                                tables.add(column2.getTable());
                            }
                            for (Table table : tables) {
                                if (!(table instanceof TableMate)) {
                                    continue;
                                }
                                TableMate table2 = (TableMate) table;
                                Column[] columns2 = table2.getRuleColumns();
                                if (columns2 == null) {
                                    continue;
                                }
                                boolean contains = true;
                                for (Column column2 : columns2) {
                                    if (!joinCols.contains(column2)) {
                                        contains = false;
                                        break;
                                    }
                                }
                                if (contains) {
                                    joinTableChain.add(filter);
                                }
                                for (TableFilter tf : filters) {
                                    if (tf.getTable() == table && !joinTableChain.contains(filter)) {
                                        evaluationJoinChain(tf);
                                    }
                                }

                            }
                        }
                    }
                }
            }
        }

    }

}
