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
package com.openddal.excutor.cursor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.openddal.command.dml.Select;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.ExpressionVisitor;
import com.openddal.dbobject.index.ConditionExtractor;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.MetaTable;
import com.openddal.dbobject.table.RangeTable;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.dbobject.table.TableView;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.excutor.works.QueryWorker;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class SearchCursor extends ExecutionFramework implements Cursor {

    private final TableFilter tableFilter;
    private Table table;
    private Cursor cursor;
    private boolean alwaysFalse;

    public SearchCursor(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
        this.table = tableFilter.getTable();
    }

    /**
     * Check if the result is empty for sure.
     *
     * @return true if it is
     */
    public boolean isAlwaysFalse() {
        return alwaysFalse;
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

    public Cursor find(MetaTable table) {
        ArrayList<Row> rows = table.generateRows(session, null, null);
        return new ListCursor(rows);
    }

    private Cursor find(RangeTable table) {
        long min = table.getMin(session), start = min;
        long max = table.getMax(session), end = max;
        return new RangeCursor(start, end);
    }

    private Cursor find(TableMate tableMate) {
        Expression filter = tableFilter.getFilterCondition();
        if (filter != null) {
            filter = filter.optimize(session);
        }
        Expression join = tableFilter.getJoinCondition();
        if (join != null) {
            join = join.optimize(session);
        }
        ConditionExtractor extractor = new ConditionExtractor(tableFilter);
        this.alwaysFalse = extractor.isAlwaysFalse();
        if (extractor.isAlwaysFalse()) {
            return this;
        }
        RoutingResult result = routingHandler.doRoute(tableMate, 
                extractor.getStart(), extractor.getStart(), extractor.getInColumns());
        ObjectNode[] selectNodes = result.getSelectNodes();
        if (session.getDatabase().getSettings().optimizeMerging) {
            selectNodes = result.group();
        }
        List<QueryWorker> workers = New.arrayList(selectNodes.length);
        for (ObjectNode objectNode : selectNodes) {
            QueryWorker worker = queryHandlerFactory.createQueryWorker(tableFilter, objectNode);
            workers.add(worker);
        }
        return invokeQueryWorker(workers);
    }

    private Cursor find(TableView tableView) {
        return null;
    }

    protected Cursor doQuery() {
        if (table instanceof RangeTable) {
            RangeTable rangeTable = (RangeTable) table;
            this.cursor = find(rangeTable);
        } else if (table instanceof MetaTable) {
            MetaTable metaTable = (MetaTable) table;
            this.cursor = find(metaTable);
        } else if (table instanceof TableMate) {
            TableMate tableMate = (TableMate) table;
            this.cursor = find(tableMate);
        } else if (table instanceof TableView) {
            TableView tableView = (TableView) table;
            this.cursor = find(tableView);
        } else {
            DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, table.getClass().getName());
        }
        return this;
    }

    protected void doPrepare() {
        HashSet<Column> columns = New.hashSet();
        Select select = tableFilter.getSelect();
        if(select != null) {
            select.isEverything(ExpressionVisitor.getColumnsVisitor(columns));
        } else {
            tableFilter.getTable().getColumns();
        }
    }

    @Override
    protected String doExplain() {
        return null;
    }
}
