package com.openddal.excutor.cursor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.openddal.command.dml.Select;
import com.openddal.command.expression.Expression;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.ConditionExtractor;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableFilter.TableFilterVisitor;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Constants;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.excutor.works.QueryWorker;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DirectLookupCursor extends ExecutionFramework<Select> implements Cursor {

    private Cursor cursor;
    private Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes;
    private List<QueryWorker> workers;
    private boolean alwaysFalse;

    public DirectLookupCursor(Select select) {
        super(select);
    }

    @Override
    protected void doPrepare() {
        ArrayList<TableFilter> topFilters = prepared.getTopFilters();
        for (TableFilter tf : topFilters) {
            ConditionExtractor extractor = new ConditionExtractor(tf);
            boolean alwaysFalse = extractor.isAlwaysFalse();
            if (alwaysFalse) {
                this.alwaysFalse = alwaysFalse;
                return;
            }
        }
        ArrayList<Expression> expressions = prepared.getExpressions();
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        Integer limit = null, offset = null;
        Expression limitExpr = prepared.getLimit();
        Expression offsetExpr = prepared.getOffset();
        if (limitExpr != null) {
            limit = limitExpr.getValue(session).getInt();
        }
        if (offsetExpr != null) {
            offset = offsetExpr.getValue(session).getInt();
        }
        RoutingResult rr = doRoute(prepared);
        if (rr.isMultipleNode() && offset != null) {
            if (limit != null && limit > database.getSettings().analyzeSample) {
                throw DbException.get(ErrorCode.INVALID_VALUE_2, "limit", limit + ", the max support limit "
                        + database.getSettings().analyzeSample + " is defined by analyzeSample.");
            }
            offset = offset != null ? 0 : offset;
        }
        ObjectNode[] selectNodes = rr.getSelectNodes();
        if (session.getDatabase().getSettings().optimizeMerging) {
            selectNodes = rr.group();
        }
        workers = New.arrayList(selectNodes.length);
        for (ObjectNode node : selectNodes) {
            QueryWorker queryHandler = queryHandlerFactory.createQueryWorker(prepared, node, consistencyTableNodes,
                    exprList, limit, offset);
            workers.add(queryHandler);
        }
    }

    @Override
    protected Cursor doQuery() {
        if (alwaysFalse) {
            return ResultCursor.EMPTY_CURSOR;
        }
        this.cursor = invokeQueryWorker(workers);
        return this;
    }

    @Override
    public String doExplain() {
        return explainForWorker(workers);
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
                ConditionExtractor extractor = new ConditionExtractor(tf);
                RoutingResult r = routingHandler.doRoute(table, extractor.getStart(), extractor.getEnd(),
                        extractor.getInColumns());
                result = (result == null || r.compareTo(result) < 0) ? r : result;
            }
        } else if (!fixeds.isEmpty()) {
            for (TableFilter tf : shards) {
                TableMate table = getTableMate(tf);
                RoutingResult r = routingHandler.doRoute(table);
                result = r;
            }
        } else if (!globals.isEmpty()) {
            // 全部为全局表查询，随机取一个第一个表结点
            GlobalTableRule tableRule = (GlobalTableRule) getTableRule(globals.iterator().next());
            RoutingResult r = tableRule.getRandomRoutingResult();
            result = r;
        } else {
            throw DbException.throwInternalError("SQL_ROUTING_ERROR");
        }
        ObjectNode[] selectNodes = result.getSelectNodes();
        if (selectNodes.length == 0) {
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
        return workers.size() * Constants.COST_ROW_OFFSET;
    }

    public static boolean isDirectLookupQuery(Select select) {
        DirectLookupEstimator estimator = new DirectLookupEstimator(select.getTopFilters());
        return estimator.isDirectLookup();
    }

    private static class DirectLookupEstimator {

        private final ArrayList<TableFilter> filters;
        private Set<TableFilter> joinTableChain;

        private DirectLookupEstimator(ArrayList<TableFilter> topFilters) {
            this.filters = New.arrayList();
            for (TableFilter tf : topFilters) {
                tf.visit(new TableFilterVisitor() {
                    @Override
                    public void accept(TableFilter f) {
                        if (!StringUtils.startsWith(f.getTableAlias(), Constants.PREFIX_JOIN)) {
                            filters.add(f);
                        }
                    }
                });

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
            List<IndexCondition> masks = New.arrayList(10);
            List<Column> compareColumns = New.arrayList(10);
            for (Column column : columns1) {
                for (IndexCondition condition : conditions) {
                    Column compareColumn = condition.getCompareColumn();
                    if ((condition.getMask(conditions) & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {
                        continue;
                    }
                    if (condition.getColumn() != column || compareColumn == null) {
                        continue;
                    }
                    masks.add(condition);
                    compareColumns.add(compareColumn);
                }
            }

            Set<Table> tables = New.hashSet();
            for (IndexCondition mask : masks) {
                Column compareColumn = mask.getCompareColumn();
                Table table = compareColumn.getTable();
                if (!(table instanceof TableMate)) {
                    continue;
                }
                TableMate tableMate = (TableMate) table;
                Column[] rc = tableMate.getRuleColumns();
                if (compareColumns.containsAll(Arrays.asList(rc))) {
                    tables.add(table);
                }
            }
            if (tables.isEmpty()) {
                return;
            }
            for (Table table : tables) {
                for (TableFilter tf : filters) {
                    if (tf.getTable() == table && !joinTableChain.contains(tf)) {
                        joinTableChain.add(tf);
                        evaluationJoinChain(tf);
                    }
                }

            }

        }

    }

}
