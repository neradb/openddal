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
package com.openddal.dbobject.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import com.openddal.command.Prepared;
import com.openddal.command.dml.Query;
import com.openddal.command.expression.Alias;
import com.openddal.command.expression.Comparison;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ExpressionColumn;
import com.openddal.command.expression.ExpressionVisitor;
import com.openddal.command.expression.Parameter;
import com.openddal.dbobject.DbObject;
import com.openddal.dbobject.User;
import com.openddal.dbobject.index.Index;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Constants;
import com.openddal.engine.Session;
import com.openddal.executor.cursor.ViewCursor;
import com.openddal.message.DbException;
import com.openddal.result.LocalResult;
import com.openddal.util.IntArray;
import com.openddal.util.New;
import com.openddal.util.SmallLRUCache;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * A view is a virtual table that is defined by a query.
 *
 * @author Thomas Mueller
 * @author Nicolas Fortin, Atelier SIG, IRSTV FR CNRS 24888
 */
public class TableView extends Table {

    private static final long ROW_COUNT_APPROXIMATION = 100;

    private String querySQL;
    private ArrayList<Table> tables;
    private String[] columnNames;
    private Query viewQuery;
    private boolean recursive;
    private DbException createException;
    private final SmallLRUCache<CacheKey, Query> queryCache =
            SmallLRUCache.newInstance(Constants.VIEW_INDEX_CACHE_SIZE);
    private User owner;
    private Query topQuery;
    private LocalResult recursiveResult;
    private boolean tableExpression;

    public TableView(Schema schema, int id, String name, String querySQL, ArrayList<Parameter> params,
            String[] columnNames, Session session, boolean recursive) {
        super(schema, name);
        init(querySQL, params, columnNames, session, recursive);
    }

    private static Query compileViewQuery(Session session, String sql) {
        Prepared p = session.prepare(sql);
        if (!(p instanceof Query)) {
            throw DbException.getSyntaxError(sql, 0);
        }
        return (Query) p;
    }

    /**
     * Create a temporary view out of the given query.
     *
     * @param session the session
     * @param owner the owner of the query
     * @param name the view name
     * @param query the query
     * @param topQuery the top level query
     * @return the view table
     */
    public static TableView createTempView(Session session, User owner, String name, Query query, Query topQuery) {
        Schema mainSchema = session.getDatabase().getSchema(Constants.SCHEMA_MAIN);
        String querySQL = query.getPlanSQL();
        TableView v = new TableView(mainSchema, 0, name, querySQL, query.getParameters(), null, session, false);
        if (v.createException != null) {
            throw v.createException;
        }
        v.setTopQuery(topQuery);
        v.setOwner(owner);
        v.setTemporary(true);
        return v;
    }

    private synchronized void init(String querySQL, ArrayList<Parameter> params, String[] columnNames, Session session,
            boolean recursive) {
        this.querySQL = querySQL;
        this.columnNames = columnNames;
        this.recursive = recursive;
        initColumnsAndTables(session);
    }

    private void initColumnsAndTables(Session session) {
        Column[] cols;
        try {
            Query query = compileViewQuery(session, querySQL);
            this.querySQL = query.getSQL();
            tables = New.arrayList(query.getTables());
            ArrayList<Expression> expressions = query.getExpressions();
            ArrayList<Column> list = New.arrayList();
            for (int i = 0, count = query.getColumnCount(); i < count; i++) {
                Expression expr = expressions.get(i);
                String name = null;
                if (columnNames != null && columnNames.length > i) {
                    name = columnNames[i];
                }
                if (name == null) {
                    name = expr.getAlias();
                }
                int type = expr.getType();
                long precision = expr.getPrecision();
                int scale = expr.getScale();
                int displaySize = expr.getDisplaySize();
                Column col = new Column(name, type, precision, scale, displaySize);
                col.setTable(this, i);
                // Fetch check constraint from view column source
                ExpressionColumn fromColumn = null;
                if (expr instanceof ExpressionColumn) {
                    fromColumn = (ExpressionColumn) expr;
                } else if (expr instanceof Alias) {
                    Expression aliasExpr = expr.getNonAliasExpression();
                    if (aliasExpr instanceof ExpressionColumn) {
                        fromColumn = (ExpressionColumn) aliasExpr;
                    }
                }
                if (fromColumn != null) {
                    Expression checkExpression = fromColumn.getColumn().getCheckConstraint(session, name);
                    if (checkExpression != null) {
                        col.addCheckConstraint(session, checkExpression);
                    }
                }
                list.add(col);
            }
            cols = new Column[list.size()];
            list.toArray(cols);
            createException = null;
            viewQuery = query;
        } catch (DbException e) {
            e.addSQL(querySQL);
            createException = e;
            // if it can't be compiled, then it's a 'zero column table'
            // this avoids problems when creating the view when opening the
            // database
            tables = New.arrayList();
            cols = new Column[0];
            if (recursive && columnNames != null) {
                cols = new Column[columnNames.length];
                for (int i = 0; i < columnNames.length; i++) {
                    cols[i] = new Column(columnNames[i], Value.STRING);
                }
                // index.setRecursive(true);
                createException = null;
            }
        }
        setColumns(cols);
    }

    /**
     * Check if this view is currently invalid.
     *
     * @return true if it is
     */
    public boolean isInvalid() {
        return createException != null;
    }
    
    @Override
    public PlanItem getBestPlanItem(Session session, int[] masks,
            TableFilter filter) {
        PlanItem item = new PlanItem();
        Query q = getCachedQuery(session, masks);
        item.cost = q.getCost();            
        return item;
    }
    
    public ViewCursor getViewCursor(Session session, TableFilter filter) {
        int len = getColumns().length;
        int[] masks = new int[len];
        ArrayList<IndexCondition> indexConditions = filter.getIndexConditions();
        for (IndexCondition condition : indexConditions) {
            if (condition.isAlwaysFalse()) {
                masks = null;
                break;
            }
            int id = condition.getColumn().getColumnId();
            if (id >= 0) {
                masks[id] |= condition.getMask(indexConditions);
            }
        }
        
        Query q = getCachedQuery(session, masks);
        ArrayList<Parameter> paramList = q.getParameters();
        ArrayList<Parameter> originalParameters = viewQuery.getParameters();
        if (originalParameters != null) {
            for (int i = 0, size = originalParameters.size(); i < size; i++) {
                Parameter orig = originalParameters.get(i);
                int idx = orig.getIndex();
                Value value = orig.getValue(session);
                setParameter(paramList, idx, value);
            }
        }
        
        int idx = originalParameters == null ? 0 : originalParameters.size();
        idx += getParameterOffset();
        for (IndexCondition condition : indexConditions) {
            int id = condition.getColumn().getColumnId();
            if (id >= 0) {
                int mask = masks[id];
                Value value = condition.getCurrentValue(session);
                if ((mask & IndexCondition.EQUALITY) != 0) {
                    setParameter(paramList, idx++, value);
                }
                if ((mask & IndexCondition.START) != 0) {
                    setParameter(paramList, idx++, value);
                }
                if ((mask & IndexCondition.END) != 0) {
                    setParameter(paramList, idx++, value);
                }
            }
        }
        LocalResult result = q.query(0);
        return new ViewCursor(this, result);
    }



    @Override
    public boolean isQueryComparable() {
        if (!super.isQueryComparable()) {
            return false;
        }
        for (Table t : tables) {
            if (!t.isQueryComparable()) {
                return false;
            }
        }
        return !(topQuery != null && !topQuery.isEverything(ExpressionVisitor.QUERY_COMPARABLE_VISITOR));
    }

    @Override
    public void checkRename() {
        // ok
    }

    @Override
    public String getTableType() {
        return Table.VIEW;
    }

    @Override
    public String getSQL() {
        if (isTemporary()) {
            return "(\n" + StringUtils.indent(querySQL) + ")";
        }
        return super.getSQL();
    }

    public String getQuery() {
        return querySQL;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return null;
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    public User getOwner() {
        return owner;
    }

    private void setOwner(User owner) {
        this.owner = owner;
    }

    private void setTopQuery(Query topQuery) {
        this.topQuery = topQuery;
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }

    public int getParameterOffset() {
        return topQuery == null ? 0 : topQuery.getParameters().size();
    }

    @Override
    public boolean isDeterministic() {
        if (recursive || viewQuery == null) {
            return false;
        }
        return viewQuery.isEverything(ExpressionVisitor.DETERMINISTIC_VISITOR);
    }

    public LocalResult getRecursiveResult() {
        return recursiveResult;
    }

    public void setRecursiveResult(LocalResult value) {
        if (recursiveResult != null) {
            recursiveResult.close();
        }
        this.recursiveResult = value;
    }

    public boolean isTableExpression() {
        return tableExpression;
    }

    public void setTableExpression(boolean tableExpression) {
        this.tableExpression = tableExpression;
    }
    

    public boolean isRecursive() {
        return recursive;
    }

    @Override
    public void addDependencies(HashSet<DbObject> dependencies) {
        super.addDependencies(dependencies);
        if (tables != null) {
            for (Table t : tables) {
                if (!Table.VIEW.equals(t.getTableType())) {
                    t.addDependencies(dependencies);
                }
            }
        }
    }
    
    private Query getCachedQuery(Session session, int[] masks) {
        final CacheKey cacheKey = new CacheKey(masks, session);
        synchronized (this) {
            Query q = queryCache.get(cacheKey);
            if (q == null) {
                q = getQuery(session, masks);
                queryCache.put(cacheKey, q);
            }
            return q;
        }
    }
    
    private Query getQuery(Session session, int[] masks) {
        Query q = (Query) session.prepare(querySQL, true);
        if (masks == null) {
            return q;
        }
        if (!q.allowGlobalConditions()) {
            return q;
        }
        int firstIndexParam = q.getParameters() == null ?
                0 : q.getParameters().size();
        firstIndexParam += getParameterOffset();
        IntArray paramIndex = new IntArray();
        for (int i = 0; i < masks.length; i++) {
            int mask = masks[i];
            if (mask == 0) {
                continue;
            }
            paramIndex.add(i);
            if (Integer.bitCount(mask) > 1) {
                // two parameters for range queries: >= x AND <= y
                paramIndex.add(i);
            }
        }
        int len = paramIndex.size();
        for (int i = 0; i < len;) {
            int idx = paramIndex.get(i);
            int mask = masks[idx];
            if ((mask & IndexCondition.EQUALITY) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.EQUAL);
                i++;
            }
            if ((mask & IndexCondition.START) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.BIGGER_EQUAL);
                i++;
            }
            if ((mask & IndexCondition.END) != 0) {
                Parameter param = new Parameter(firstIndexParam + i);
                q.addGlobalCondition(param, idx, Comparison.SMALLER_EQUAL);
                i++;
            }
        }

        String sql = q.getPlanSQL();
        q = (Query) session.prepare(sql, true);
        return q;
    }
    
    private static void setParameter(ArrayList<Parameter> paramList, int x,
            Value v) {
        if (x >= paramList.size()) {
            // the parameter may be optimized away as in
            // select * from (select null as x) where x=1;
            return;
        }
        Parameter param = paramList.get(x);
        param.setValue(v);
    }
    
    /**
     * The key of the index cache for views.
     */
    private static final class CacheKey {

        private final int[] masks;
        private final Session session;

        public CacheKey(int[] masks, Session session) {
            this.masks = masks;
            this.session = session;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Arrays.hashCode(masks);
            result = prime * result + session.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            CacheKey other = (CacheKey) obj;
            if (session != other.session) {
                return false;
            }
            if (!Arrays.equals(masks, other.masks)) {
                return false;
            }
            return true;
        }
    }
}
