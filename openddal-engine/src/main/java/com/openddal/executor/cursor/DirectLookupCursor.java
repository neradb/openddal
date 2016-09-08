package com.openddal.executor.cursor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.openddal.command.dml.Select;
import com.openddal.command.expression.Aggregate;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ExpressionVisitor;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.ConditionExtractor;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.RangeTable;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableFilter.TableFilterVisitor;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Constants;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.QueryWorker;
import com.openddal.message.DbException;
import com.openddal.result.LocalResult;
import com.openddal.result.ResultTarget;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.result.SortOrder;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class DirectLookupCursor extends ExecutionFramework implements Cursor {

    private Select prepared;
    private Cursor cursor;
    private Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes;
    private List<QueryWorker> workers;
    private ArrayList<Expression> expressions;
    private boolean limitPushless;

    public DirectLookupCursor(Select select) {
        this.prepared = select;
    }

    @Override
    protected void doPrepare() {
        expressions = prepared.getExpressions();
        if (prepared.isGroupQuery()) {
            ArrayList<Expression> selectExprs = New.arrayList(10);
            int[] groupIndex = prepared.getGroupIndex();
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                int idx = groupIndex[i];
                Expression expr = expressions.get(idx);
                selectExprs.add(expr);
            }
            HashSet<Aggregate> aggregates = New.linkedHashSet();
            for (Expression expr : expressions) {
                expr.isEverything(ExpressionVisitor.getAggregateVisitor(aggregates));
            }
            selectExprs.addAll(aggregates);
            expressions = selectExprs;
        }
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
        
        
        try {
            setEvaluatable(prepared.getTopTableFilter(), false);
            RoutingResult rr = doRoute(prepared);
            if (rr.isMultipleNode() && offset != null) {
                limit = limit == null ? null : limit + offset;
                offset = 0;
                limitPushless = true;
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
        } finally {
            setEvaluatable(prepared.getTopTableFilter(), true);
        }
    }

    @Override
    protected Cursor doQuery() {
        this.cursor = invokeQueryWorker(workers);
        return this;
    }

    @Override
    public String doExplain() {
        return explainForWorker(workers);
    }

    private RoutingResult doRoute(Select prepare) {
        List<TableFilter> filters = filterNotTableMate(prepare.getTopTableFilter());
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
            for (TableFilter f : shards) {
                if (f.isJoinOuter() || f.isJoinOuterIndirect()) {
                    prepare.getCondition().createIndexConditions(session, f);
                }
                TableMate table = getTableMate(f);
                ConditionExtractor extractor = new ConditionExtractor(f);
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
        return false;
    }

    public HashMap<Expression, Value> getCurrentValues() {
        SearchRow searchRow = cursor.getSearchRow();
        int len = expressions.size();
        HashMap<Expression, Value> result = New.hashMapNonRehash(len);
        for (int i = 0; i < len; i++) {
            result.put(expressions.get(i), searchRow.getValue(i));
        }
        return result;
    }
    
    
    public void resetResult(ResultTarget result) {
        if(!isPrepared()) {
            throw DbException.throwInternalError("executor not prepared.");
        }
        if (result instanceof LocalResult) {
            LocalResult r = (LocalResult) result;
            if (limitPushless) {
                Expression offsetExpr = prepared.getOffset();
                SortOrder sortOrder = prepared.getSortOrder();
                int offset = offsetExpr.getValue(session).getInt();
                if(sortOrder == null) {
                    //drop offset rows if sortOrder is null
                    while (offset-- > 0) {
                        if (!next()) {
                            break;
                        }
                    }
                    r.setOffset(0);
                }
            } else {
                r.setOffset(0);
            }
        }
    }

    public double getCost() {
        return workers.size() * Constants.COST_ROW_OFFSET;
    }
    
    private void setEvaluatable(TableFilter f, boolean evaluateable) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, evaluateable);
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatable(n, evaluateable);
            }
        }
    }

    public static boolean isDirectLookupQuery(Select select) {
        DirectLookupEstimator estimator = new DirectLookupEstimator(select.getTopFilters());
        return estimator.isDirectLookup();
    }

    private static class DirectLookupEstimator {

        private final ArrayList<TableFilter> filters;
        private final ArrayList<Expression> joinCond;
        private Set<TableFilter> joinTableChain;

        private DirectLookupEstimator(ArrayList<TableFilter> topFilters) {
            this.filters = New.arrayList();
            this.joinCond = New.arrayList();
            for (TableFilter f : topFilters) {
                f.visit(new TableFilterVisitor() {
                    @Override
                    public void accept(TableFilter f) {
                        filters.add(f);
                        if (f.getJoinCondition() != null) {
                            joinCond.add(f.getJoinCondition());
                        }
                    }
                });
            }
            
        }

        private boolean isDirectLookup() {
            for (TableFilter tf : filters) {
                if (!tf.isFromTableMate() && !isNestedJoinTable(tf)) {
                    return false;
                }
            }
            if (filters.size() == 1) {
                return true;
            } else {
                List<TableFilter> shardingTableFilter = New.arrayList();
                for (TableFilter tf : filters) {
                    if(isNestedJoinTable(tf)) {
                        continue;
                    }
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
                if (item == filter || isNestedJoinTable(item)) {
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
            ArrayList<IndexCondition> conditions = getIndexConditions(filter);
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
        
        private boolean isNestedJoinTable(TableFilter f) {
            return f.getTable() instanceof RangeTable 
                    && StringUtils.startsWith(f.getTableAlias(), Constants.PREFIX_JOIN);
        }

        private ArrayList<IndexCondition> getIndexConditions(TableFilter filter) {
            ArrayList<IndexCondition> indexConditions = filter.getIndexConditions();
            if(joinCond.isEmpty()) {
                return indexConditions;
            }
            ArrayList<IndexCondition> original = New.arrayList(indexConditions);
            ArrayList<IndexCondition> result;
            try {
                for (Expression cond : joinCond) {
                    //add to indexConditions 
                    cond.createIndexConditions(filter.getSession(), filter);
                } 
                result = New.arrayList(indexConditions);
                return result;
            } finally {
                indexConditions.clear();
                indexConditions.addAll(original);
            }
        }

    }

}
