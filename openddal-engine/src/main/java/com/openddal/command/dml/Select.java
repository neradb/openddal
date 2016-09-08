/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package com.openddal.command.dml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.openddal.command.CommandInterface;
import com.openddal.command.expression.Comparison;
import com.openddal.command.expression.ConditionAndOr;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ExpressionColumn;
import com.openddal.command.expression.ExpressionVisitor;
import com.openddal.command.expression.Parameter;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.ColumnResolver;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.engine.SysProperties;
import com.openddal.executor.cursor.DirectLookupCursor;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.LocalResult;
import com.openddal.result.ResultInterface;
import com.openddal.result.ResultTarget;
import com.openddal.result.Row;
import com.openddal.result.SortOrder;
import com.openddal.util.New;
import com.openddal.util.StatementBuilder;
import com.openddal.util.StringUtils;
import com.openddal.util.ValueHashMap;
import com.openddal.value.Value;
import com.openddal.value.ValueArray;
import com.openddal.value.ValueNull;

/**
 * This class represents a simple SELECT statement.
 *
 * For each select statement, visibleColumnCount &lt;= distinctColumnCount &lt;=
 * expressionCount. The expression list count could include ORDER BY and GROUP
 * BY expressions that are not in the select list.
 *
 * The call sequence is init(), mapColumns() if it's a subquery, prepare().
 *
 * @author Thomas Mueller
 * @author Joel Turkel (Group sorted query)
 */
public class Select extends Query {
    private TableFilter topTableFilter;
    private final ArrayList<TableFilter> filters = New.arrayList();
    private final ArrayList<TableFilter> topFilters = New.arrayList();
    private ArrayList<Expression> expressions;
    private Expression[] expressionArray;
    private Expression having;
    private Expression condition;
    private int visibleColumnCount, distinctColumnCount;
    private ArrayList<SelectOrderBy> orderList;
    private ArrayList<Expression> group;
    private int[] groupIndex;
    private boolean[] groupByExpression;
    private HashMap<Expression, Object> currentGroup;
    private HashMap<Expression, Value> currentValues;
    private int havingIndex;
    private boolean isGroupQuery;
    private boolean isForUpdate, isForUpdateMvcc;
    private double cost;
    private boolean isPrepared, checkInit;
    private SortOrder sort;
    private int currentGroupRowId;
    private boolean isDirectLookupQuery;

    public Select(Session session) {
        super(session);
    }

    /**
     * Add a table to the query.
     *
     * @param filter the table to add
     * @param isTop if the table can be the first table in the query plan
     */
    public void addTableFilter(TableFilter filter, boolean isTop) {
        // Oracle doesn't check on duplicate aliases
        // String alias = filter.getAlias();
        // if (filterNames.contains(alias)) {
        // throw Message.getSQLException(
        // ErrorCode.DUPLICATE_TABLE_ALIAS, alias);
        // }
        // filterNames.add(alias);
        filters.add(filter);
        if (isTop) {
            topFilters.add(filter);
        }
    }

    public ArrayList<TableFilter> getTopFilters() {
        return topFilters;
    }

    public void setExpressions(ArrayList<Expression> expressions) {
        this.expressions = expressions;
    }

    /**
     * Called if this query contains aggregate functions.
     */
    public void setGroupQuery() {
        isGroupQuery = true;
    }

    public void setGroupBy(ArrayList<Expression> group) {
        this.group = group;
    }

    public ArrayList<Expression> getGroupBy() {
        return group;
    }

    public HashMap<Expression, Object> getCurrentGroup() {
        return currentGroup;
    }

    public int getCurrentGroupRowId() {
        return currentGroupRowId;
    }

    public HashMap<Expression, Value> getCurrentValues() {
        return currentValues;
    }

    @Override
    public void setOrder(ArrayList<SelectOrderBy> order) {
        orderList = order;
    }

    /**
     * Add a condition to the list of conditions.
     *
     * @param cond the condition to add
     */
    public void addCondition(Expression cond) {
        if (condition == null) {
            condition = cond;
        } else {
            condition = new ConditionAndOr(ConditionAndOr.AND, cond, condition);
        }
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public int[] getGroupIndex() {
        return groupIndex;
    }

    public boolean isForUpdate() {
        return isForUpdate;
    }

    private Value[] keepOnlyDistinct(Value[] row, int columnCount) {
        if (columnCount == distinctColumnCount) {
            return row;
        }
        // remove columns so that 'distinct' can filter duplicate rows
        Value[] r2 = new Value[distinctColumnCount];
        System.arraycopy(row, 0, r2, 0, distinctColumnCount);
        return r2;
    }

    private boolean isHavingNullOrFalse(Value[] row) {
        if (havingIndex >= 0) {
            Value v = row[havingIndex];
            if (v == ValueNull.INSTANCE) {
                return true;
            }
            if (!Boolean.TRUE.equals(v.getBoolean())) {
                return true;
            }
        }
        return false;
    }

    private void queryGroup(int columnCount, LocalResult result) {
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance();
        int rowNumber = 0;
        setCurrentRowNumber(0);
        currentGroup = null;
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value key;
                rowNumber++;
                if (groupIndex == null) {
                    key = defaultGroup;
                } else {
                    Value[] keyValues = new Value[groupIndex.length];
                    // update group
                    for (int i = 0; i < groupIndex.length; i++) {
                        int idx = groupIndex[i];
                        Expression expr = expressions.get(idx);
                        keyValues[i] = expr.getValue(session);
                    }
                    key = ValueArray.get(keyValues);
                }
                HashMap<Expression, Object> values = groups.get(key);
                if (values == null) {
                    values = new HashMap<Expression, Object>();
                    groups.put(key, values);
                }
                currentGroup = values;
                currentGroupRowId++;
                int len = columnCount;
                for (int i = 0; i < len; i++) {
                    if (groupByExpression == null || !groupByExpression[i]) {
                        Expression expr = expressions.get(i);
                        expr.updateAggregate(session);
                    }
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row)) {
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }
    }

    private void queryFlat(int columnCount, ResultTarget result, long limitRows) {
        // limitRows must be long, otherwise we get an int overflow
        // if limitRows is at or near Integer.MAX_VALUE
        // limitRows is never 0 here
        if (limitRows > 0 && offsetExpr != null) {
            int offset = offsetExpr.getValue(session).getInt();
            if (offset > 0) {
                limitRows += offset;
            }
        }
        int rowNumber = 0;
        setCurrentRowNumber(0);
        ArrayList<Row> forUpdateRows = null;
        if (isForUpdateMvcc) {
            forUpdateRows = New.arrayList();
        }
        int sampleSize = getSampleSizeValue(session);
        while (topTableFilter.next()) {
            setCurrentRowNumber(rowNumber + 1);
            if (condition == null || Boolean.TRUE.equals(condition.getBooleanValue(session))) {
                Value[] row = new Value[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    Expression expr = expressions.get(i);
                    row[i] = expr.getValue(session);
                }
                if (isForUpdateMvcc) {
                    topTableFilter.lockRowAdd(forUpdateRows);
                }
                result.addRow(row);
                rowNumber++;
                if (sort == null && limitRows > 0 && result.getRowCount() >= limitRows) {
                    break;
                }
                if (sampleSize > 0 && rowNumber >= sampleSize) {
                    break;
                }
            }
        }
        if (isForUpdateMvcc) {
            topTableFilter.lockRows(forUpdateRows);
        }
    }

    private void queryQuick(int columnCount, ResultTarget result, long limitRows) {
        int rowNumber = 0;
        setCurrentRowNumber(0);
        DirectLookupCursor lookupCursor = new DirectLookupCursor(this);
        lookupCursor.query(session);
        lookupCursor.resetResult(result);
        while (lookupCursor.next()) {
            setCurrentRowNumber(rowNumber + 1);
            Row searchRow = lookupCursor.get();
            Value[] row = new Value[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = searchRow.getValue(i);
            }
            result.addRow(row);
            rowNumber++;
            if (sort == null && limitRows > 0 && result.getRowCount() >= limitRows) {
                break;
            }
        }

    }

    private void queryGroupQuick(int columnCount, ResultTarget result) {
        ValueHashMap<HashMap<Expression, Object>> groups = ValueHashMap.newInstance();
        int rowNumber = 0;
        setCurrentRowNumber(0);
        currentGroup = null;
        currentValues = null;
        ValueArray defaultGroup = ValueArray.get(new Value[0]);
        int sampleSize = getSampleSizeValue(session);
        DirectLookupCursor lookupCursor = new DirectLookupCursor(this);
        lookupCursor.query(session);
        while (lookupCursor.next()) {
            setCurrentRowNumber(rowNumber + 1);
            Value key;
            rowNumber++;
            currentValues = lookupCursor.getCurrentValues();
            if (groupIndex == null) {
                key = defaultGroup;
            } else {
                Value[] keyValues = new Value[groupIndex.length];
                // update group
                for (int i = 0; i < groupIndex.length; i++) {
                    int idx = groupIndex[i];
                    Expression expr = expressions.get(idx);
                    keyValues[i] = currentValues.get(expr);
                }
                key = ValueArray.get(keyValues);
            }
            HashMap<Expression, Object> values = groups.get(key);
            if (values == null) {
                values = new HashMap<Expression, Object>();
                groups.put(key, values);
            }
            currentGroup = values;
            currentGroupRowId++;
            int len = columnCount;
            for (int i = 0; i < len; i++) {
                if (groupByExpression == null || !groupByExpression[i]) {
                    Expression expr = expressions.get(i);
                    expr.updateAggregate(session);
                }
            }
            if (sampleSize > 0 && rowNumber >= sampleSize) {
                break;
            }

        }
        if (groupIndex == null && groups.size() == 0) {
            groups.put(defaultGroup, new HashMap<Expression, Object>());
        }
        ArrayList<Value> keys = groups.keys();
        for (Value v : keys) {
            ValueArray key = (ValueArray) v;
            currentGroup = groups.get(key);
            Value[] keyValues = key.getList();
            Value[] row = new Value[columnCount];
            for (int j = 0; groupIndex != null && j < groupIndex.length; j++) {
                row[groupIndex[j]] = keyValues[j];
            }
            for (int j = 0; j < columnCount; j++) {
                if (groupByExpression != null && groupByExpression[j]) {
                    continue;
                }
                Expression expr = expressions.get(j);
                row[j] = expr.getValue(session);
            }
            if (isHavingNullOrFalse(row)) {
                continue;
            }
            row = keepOnlyDistinct(row, columnCount);
            result.addRow(row);
        }

    }

    @Override
    public ResultInterface queryMeta() {
        LocalResult result = new LocalResult(session, expressionArray, visibleColumnCount);
        result.done();
        return result;
    }

    @Override
    protected LocalResult queryWithoutCache(int maxRows, ResultTarget target) {
        int limitRows = maxRows == 0 ? -1 : maxRows;
        if (limitExpr != null) {
            Value v = limitExpr.getValue(session);
            int l = v == ValueNull.INSTANCE ? -1 : v.getInt();
            if (limitRows < 0) {
                limitRows = l;
            } else if (l >= 0) {
                limitRows = Math.min(l, limitRows);
            }
        }
        // If the target is not null and the query without sort,distinct group
        // by etc.. not creating LocalResult, using target and returning null
        int columnCount = expressions.size();
        LocalResult result = null;
        if (target == null || !session.getDatabase().getSettings().optimizeInsertFromSelect) {
            result = createLocalResult(result);
        }
        if (sort != null) {
            result = createLocalResult(result);
            result.setSortOrder(sort);
        }
        if (distinct) {
            result = createLocalResult(result);
            result.setDistinct();
        }
        if (randomAccessResult) {
            result = createLocalResult(result);
        }
        if (isGroupQuery) {
            result = createLocalResult(result);
        }        
        if (limitRows >= 0) {
            result = createLocalResult(result);
            result.setLimit(limitRows);
        }
        if (offsetExpr != null) {
            result = createLocalResult(result);
            result.setOffset(offsetExpr.getValue(session).getInt());
        }
        
        topTableFilter.startQuery(session);
        topTableFilter.reset();
        boolean exclusive = isForUpdate && !isForUpdateMvcc;
        if (isForUpdateMvcc) {
            if (isGroupQuery) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && GROUP");
            } else if (distinct) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && DISTINCT");
            } else if (topTableFilter.getJoin() != null) {
                throw DbException.getUnsupportedException("MVCC=TRUE && FOR UPDATE && JOIN");
            }
        }
        topTableFilter.lock(session, exclusive, exclusive);
        ResultTarget to = result != null ? result : target;
        if (limitRows != 0) {
            if (isDirectLookupQuery) {
                if (isGroupQuery) {
                    queryGroupQuick(columnCount, to);
                } else {
                    queryQuick(columnCount, to, limitRows);
                }
            } else if (isGroupQuery) {
                queryGroup(columnCount, result);
            } else {
                queryFlat(columnCount, to, limitRows);
            }
        }
        if (result != null) {
            result.done();
            if (target != null) {
                while (result.next()) {
                    target.addRow(result.currentRow());
                }
                result.close();
                return null;
            }
            return result;
        }
        return null;
    }

    private LocalResult createLocalResult(LocalResult old) {
        return old != null ? old : new LocalResult(session, expressionArray, visibleColumnCount);
    }

    private void expandColumnList() {
        Database db = session.getDatabase();

        // the expressions may change within the loop
        for (int i = 0; i < expressions.size(); i++) {
            Expression expr = expressions.get(i);
            if (!expr.isWildcard()) {
                continue;
            }
            String schemaName = expr.getSchemaName();
            String tableAlias = expr.getTableAlias();
            if (tableAlias == null) {
                expressions.remove(i);
                for (TableFilter filter : filters) {
                    i = expandColumnList(filter, i);
                }
                i--;
            } else {
                TableFilter filter = null;
                for (TableFilter f : filters) {
                    if (db.equalsIdentifiers(tableAlias, f.getTableAlias())) {
                        if (schemaName == null || db.equalsIdentifiers(schemaName, f.getSchemaName())) {
                            filter = f;
                            break;
                        }
                    }
                }
                if (filter == null) {
                    throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableAlias);
                }
                expressions.remove(i);
                i = expandColumnList(filter, i);
                i--;
            }
        }
    }

    private int expandColumnList(TableFilter filter, int index) {
        Table t = filter.getTable();
        String alias = filter.getTableAlias();
        Column[] columns = t.getColumns();
        for (Column c : columns) {
            if (filter.isNaturalJoinColumn(c)) {
                continue;
            }
            ExpressionColumn ec = new ExpressionColumn(session.getDatabase(), null, alias, c.getName());
            expressions.add(index++, ec);
        }
        return index;
    }

    @Override
    public void init() {
        if (SysProperties.CHECK && checkInit) {
            DbException.throwInternalError();
        }
        expandColumnList();
        visibleColumnCount = expressions.size();
        ArrayList<String> expressionSQL;
        if (orderList != null || group != null) {
            expressionSQL = New.arrayList();
            for (int i = 0; i < visibleColumnCount; i++) {
                Expression expr = expressions.get(i);
                expr = expr.getNonAliasExpression();
                String sql = expr.getSQL();
                expressionSQL.add(sql);
            }
        } else {
            expressionSQL = null;
        }
        if (orderList != null) {
            initOrder(session, expressions, expressionSQL, orderList, visibleColumnCount, distinct, filters);
        }
        distinctColumnCount = expressions.size();
        if (having != null) {
            expressions.add(having);
            havingIndex = expressions.size() - 1;
            having = null;
        } else {
            havingIndex = -1;
        }

        Database db = session.getDatabase();

        // first the select list (visible columns),
        // then 'ORDER BY' expressions,
        // then 'HAVING' expressions,
        // and 'GROUP BY' expressions at the end
        if (group != null) {
            int size = group.size();
            int expSize = expressionSQL.size();
            groupIndex = new int[size];
            for (int i = 0; i < size; i++) {
                Expression expr = group.get(i);
                String sql = expr.getSQL();
                int found = -1;
                for (int j = 0; j < expSize; j++) {
                    String s2 = expressionSQL.get(j);
                    if (db.equalsIdentifiers(s2, sql)) {
                        found = j;
                        break;
                    }
                }
                if (found < 0) {
                    // special case: GROUP BY a column alias
                    for (int j = 0; j < expSize; j++) {
                        Expression e = expressions.get(j);
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                        sql = expr.getAlias();
                        if (db.equalsIdentifiers(sql, e.getAlias())) {
                            found = j;
                            break;
                        }
                    }
                }
                if (found < 0) {
                    int index = expressions.size();
                    groupIndex[i] = index;
                    expressions.add(expr);
                } else {
                    groupIndex[i] = found;
                }
            }
            groupByExpression = new boolean[expressions.size()];
            for (int gi : groupIndex) {
                groupByExpression[gi] = true;
            }
            group = null;
        }
        // map columns in select list and condition
        for (TableFilter f : filters) {
            mapColumns(f, 0);
        }
        if (havingIndex >= 0) {
            Expression expr = expressions.get(havingIndex);
            SelectListColumnResolver res = new SelectListColumnResolver(this);
            expr.mapColumns(res, 0);
        }
        checkInit = true;
    }

    @Override
    public void prepare() {
        if (isPrepared) {
            // sometimes a subquery is prepared twice (CREATE TABLE AS SELECT)
            return;
        }
        if (SysProperties.CHECK && !checkInit) {
            DbException.throwInternalError("not initialized");
        }
        if (orderList != null) {
            sort = prepareOrder(orderList, expressions.size());
            orderList = null;
        }
        for (int i = 0; i < expressions.size(); i++) {
            Expression e = expressions.get(i);
            expressions.set(i, e.optimize(session));
        }
        if (condition != null) {
            condition = condition.optimize(session);
            for (TableFilter f : filters) {
                // outer joins: must not add index conditions such as
                // "c is null" - example:
                // create table parent(p int primary key) as select 1;
                // create table child(c int primary key, pc int);
                // insert into child values(2, 1);
                // select p, c from parent
                // left outer join child on p = pc where c is null;
                if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                    condition.createIndexConditions(session, f);
                }
            }
        }
        isDirectLookupQuery = DirectLookupCursor.isDirectLookupQuery(this);
        cost = preparePlan();
        expressionArray = new Expression[expressions.size()];
        expressions.toArray(expressionArray);
        isPrepared = true;
    }

    @Override
    public double getCost() {
        return cost;
    }

    @Override
    public HashSet<Table> getTables() {
        HashSet<Table> set = New.hashSet();
        for (TableFilter filter : filters) {
            set.add(filter.getTable());
        }
        return set;
    }

    private double preparePlan() {
        TableFilter[] topArray = topFilters.toArray(new TableFilter[topFilters.size()]);
        for (TableFilter t : topArray) {
            t.setFullCondition(condition);
        }

        Optimizer optimizer = new Optimizer(topArray, condition, session);
        optimizer.optimize();
        topTableFilter = optimizer.getTopFilter();
        double planCost = optimizer.getCost();
        setEvaluatableRecursive(topTableFilter);
        topTableFilter.prepare();
        return planCost;
    }

    private void setEvaluatableRecursive(TableFilter f) {
        for (; f != null; f = f.getJoin()) {
            f.setEvaluatable(f, true);
            if (condition != null) {
                condition.setEvaluatable(f, true);
            }
            TableFilter n = f.getNestedJoin();
            if (n != null) {
                setEvaluatableRecursive(n);
            }
            Expression on = f.getJoinCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    if (session.getDatabase().getSettings().nestedJoins) {
                        // need to check that all added are bound to a table
                        on = on.optimize(session);
                        if (!f.isJoinOuter() && !f.isJoinOuterIndirect()) {
                            f.removeJoinCondition();
                            addCondition(on);
                        }
                    } else {
                        if (f.isJoinOuter()) {
                            // this will check if all columns exist - it may or
                            // may not throw an exception
                            on = on.optimize(session);
                            // it is not supported even if the columns exist
                            throw DbException.get(ErrorCode.UNSUPPORTED_OUTER_JOIN_CONDITION_1, on.getSQL());
                        }
                        f.removeJoinCondition();
                        // need to check that all added are bound to a table
                        on = on.optimize(session);
                        addCondition(on);
                    }
                }
            }
            on = f.getFilterCondition();
            if (on != null) {
                if (!on.isEverything(ExpressionVisitor.EVALUATABLE_VISITOR)) {
                    f.removeFilterCondition();
                    addCondition(on);
                }
            }
            // this is only important for subqueries, so they know
            // the result columns are evaluatable
            for (Expression e : expressions) {
                e.setEvaluatable(f, true);
            }
        }
    }

    @Override
    public String explainPlan() {
        if (isDirectLookupQuery) {
            DirectLookupCursor lookupCursor = new DirectLookupCursor(this);
            return lookupCursor.explain(session);
        }
        return "cross node join";
    }

    @Override
    public String getPlanSQL() {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        StatementBuilder buff = new StatementBuilder("SELECT");
        if (distinct) {
            buff.append(" DISTINCT");
        }
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append('\n');
            buff.append(StringUtils.indent(exprList[i].getSQL(), 4, false));
        }
        buff.append("\nFROM ");
        TableFilter filter = topTableFilter;
        if (filter != null) {
            buff.resetCount();
            int i = 0;
            do {
                buff.appendExceptFirst("\n");
                buff.append(filter.getPlanSQL(i++ > 0));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : topFilters) {
                do {
                    buff.appendExceptFirst("\n");
                    buff.append(f.getPlanSQL(i++ > 0));
                    f = f.getJoin();
                } while (f != null);
            }
        }
        if (condition != null) {
            buff.append("\nWHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (groupIndex != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (group != null) {
            buff.append("\nGROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append("\nHAVING ").append(StringUtils.unEnclose(h.getSQL()));
        }
        if (sort != null) {
            buff.append("\nORDER BY ").append(sort.getSQL(exprList, visibleColumnCount));
        }
        if (orderList != null) {
            buff.append("\nORDER BY ");
            buff.resetCount();
            for (SelectOrderBy o : orderList) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(o.getSQL()));
            }
        }
        if (limitExpr != null) {
            buff.append("\nLIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append(" OFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }
        if (sampleSizeExpr != null) {
            buff.append("\nSAMPLE_SIZE ").append(StringUtils.unEnclose(sampleSizeExpr.getSQL()));
        }
        if (isForUpdate) {
            buff.append("\nFOR UPDATE");
        }
        return buff.toString();
    }

    public void setHaving(Expression having) {
        this.having = having;
    }

    public Expression getHaving() {
        return having;
    }

    public int getHavingIndex() {
        return havingIndex;
    }

    @Override
    public int getColumnCount() {
        return visibleColumnCount;
    }

    public TableFilter getTopTableFilter() {
        return topTableFilter;
    }

    @Override
    public ArrayList<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public void setForUpdate(boolean b) {
        this.isForUpdate = b;
    }

    @Override
    public void mapColumns(ColumnResolver resolver, int level) {
        for (Expression e : expressions) {
            e.mapColumns(resolver, level);
        }
        if (condition != null) {
            condition.mapColumns(resolver, level);
        }
    }

    @Override
    public void setEvaluatable(TableFilter tableFilter, boolean b) {
        for (Expression e : expressions) {
            e.setEvaluatable(tableFilter, b);
        }
        if (condition != null) {
            condition.setEvaluatable(tableFilter, b);
        }
    }

    @Override
    public void addGlobalCondition(Parameter param, int columnId, int comparisonType) {
        addParameter(param);
        Expression comp;
        Expression col = expressions.get(columnId);
        col = col.getNonAliasExpression();
        if (col.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR)) {
            comp = new Comparison(session, comparisonType, col, param);
        } else {
            // this condition will always evaluate to true, but need to
            // add the parameter, so it can be set later
            comp = new Comparison(session, Comparison.EQUAL_NULL_SAFE, param, param);
        }
        comp = comp.optimize(session);
        boolean addToCondition = true;
        if (isGroupQuery) {
            addToCondition = false;
            for (int i = 0; groupIndex != null && i < groupIndex.length; i++) {
                if (groupIndex[i] == columnId) {
                    addToCondition = true;
                    break;
                }
            }
            if (!addToCondition) {
                if (havingIndex >= 0) {
                    having = expressions.get(havingIndex);
                }
                if (having == null) {
                    having = comp;
                } else {
                    having = new ConditionAndOr(ConditionAndOr.AND, having, comp);
                }
            }
        }
        if (addToCondition) {
            if (condition == null) {
                condition = comp;
            } else {
                condition = new ConditionAndOr(ConditionAndOr.AND, condition, comp);
            }
        }
    }

    @Override
    public void updateAggregate(Session s) {
        for (Expression e : expressions) {
            e.updateAggregate(s);
        }
        if (condition != null) {
            condition.updateAggregate(s);
        }
        if (having != null) {
            having.updateAggregate(s);
        }
    }

    @Override
    public boolean isEverything(ExpressionVisitor visitor) {
        switch (visitor.getType()) {
        case ExpressionVisitor.DETERMINISTIC: {
            if (isForUpdate) {
                return false;
            }
            for (int i = 0, size = filters.size(); i < size; i++) {
                TableFilter f = filters.get(i);
                if (!f.getTable().isDeterministic()) {
                    return false;
                }
            }
            break;
        }
        case ExpressionVisitor.EVALUATABLE: {
            if (!session.getDatabase().getSettings().optimizeEvaluatableSubqueries) {
                return false;
            }
            break;
        }
        case ExpressionVisitor.GET_DEPENDENCIES: {
            for (int i = 0, size = filters.size(); i < size; i++) {
                TableFilter f = filters.get(i);
                Table table = f.getTable();
                visitor.addDependency(table);
                table.addDependencies(visitor.getDependencies());
            }
            break;
        }
        default:
        }
        ExpressionVisitor v2 = visitor.incrementQueryLevel(1);
        boolean result = true;
        for (int i = 0, size = expressions.size(); i < size; i++) {
            Expression e = expressions.get(i);
            if (!e.isEverything(v2)) {
                result = false;
                break;
            }
        }
        if (result && condition != null && !condition.isEverything(v2)) {
            result = false;
        }
        if (result && having != null && !having.isEverything(v2)) {
            result = false;
        }
        return result;
    }

    @Override
    public boolean isReadOnly() {
        return isEverything(ExpressionVisitor.READONLY_VISITOR);
    }

    @Override
    public boolean isCacheable() {
        return !isForUpdate;
    }

    @Override
    public int getType() {
        return CommandInterface.SELECT;
    }

    @Override
    public boolean allowGlobalConditions() {
        if (offsetExpr == null && (limitExpr == null || sort == null)) {
            return true;
        }
        return false;
    }

    public SortOrder getSortOrder() {
        return sort;
    }

    public boolean isGroupQuery() {
        return isGroupQuery;
    }

    public boolean isDirectLookupQuery() {
        return isDirectLookupQuery;
    }

}
