/*
 * Copyright 2014-2015 the original author or authors
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
package com.openddal.executor.effects;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.openddal.command.Prepared;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Query;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.ResultInterface;
import com.openddal.result.ResultTarget;
import com.openddal.result.Row;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class InsertExecutor extends ExecutionFramework implements ResultTarget {

    private static final int QUERY_FLUSH_THRESHOLD = 200;
    private int rowNumber;
    private int affectRows;
    private List<Row> newRows = New.arrayList(10);
    private List<UpdateWorker> workers;
    private Insert prepared;

    /**
     * @param prepared
     */
    public InsertExecutor(Insert prepared) {
        this.prepared = prepared;
    }

    @Override
    protected void doPrepare() {
        TableMate table = toTableMate(prepared.getTable());
        table.check();
        prepared.setCurrentRowNumber(0);
        ArrayList<Expression[]> list = prepared.getList();
        Column[] columns = prepared.getColumns();
        Map<Column, Expression> valueMap = prepared.getDuplicateKeyAssignmentMap();
        if (valueMap != null) {
            Column[] ruleColumns = table.getRuleColumns();
            for (Column column : ruleColumns) {
                if (valueMap.get(column) != null) {
                    throw DbException.get(ErrorCode.SHARDING_COLUMNS_CANNOT_BE_MODIFIED, column.getName());
                }
            }
        }
        int listSize = list.size();
        if (listSize > 0) {
            int columnLen = columns.length;
            List<Row> values = New.arrayList(10);
            for (int x = 0; x < listSize; x++) {
                Row newRow = table.getTemplateRow();
                Expression[] expr = list.get(x);
                prepared.setCurrentRowNumber(x + 1);
                for (int i = 0; i < columnLen; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
                        e = e.optimize(session);
                        try {
                            Value v = c.convert(e.getValue(session));
                            newRow.setValue(index, v);
                        } catch (DbException ex) {
                            throw prepared.setRow(ex, x, Prepared.getSQL(expr));
                        }
                    }
                }
                values.add(newRow);
            }
            prepareInsert(table, values);
        } else {
            Query query = prepared.getQuery();
            query.prepare();
        }
    }

    @Override
    public int doUpdate() {
        if (workers != null) {
            return invokeUpdateWorker(workers);
        } else {
            Query query = prepared.getQuery();
            if (prepared.isInsertFromSelect()) {
                query.query(0, this);
            } else {
                ResultInterface rows = query.query(0);
                while (rows.next()) {
                    Value[] r = rows.currentRow();
                    addRow(r);
                }
                rows.close();
            }
            flushNewRows();
            return this.affectRows;
        }

    }

    private void prepareInsert(TableMate table, List<Row> rows) {
        session.checkCanceled();
        Map<ObjectNode, List<Row>> batches = batchForRoutingNode(table, rows);
        workers = New.arrayList(batches.size());
        for (Map.Entry<ObjectNode, List<Row>> item : batches.entrySet()) {
            Row[] values = item.getValue().toArray(new Row[item.getValue().size()]);
            UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, item.getKey(), values);
            workers.add(worker);
        }
    }

    @Override
    public void addRow(Value[] values) {
        TableMate table = toTableMate(prepared.getTable());
        Row newRow = table.getTemplateRow();
        Column[] columns = prepared.getColumns();
        prepared.setCurrentRowNumber(++rowNumber);
        for (int j = 0, len = columns.length; j < len; j++) {
            Column c = columns[j];
            int index = c.getColumnId();
            try {
                Value v = c.convert(values[j]);
                newRow.setValue(index, v);
            } catch (DbException ex) {
                throw prepared.setRow(ex, rowNumber, Prepared.getSQL(values));
            }
        }
        addNewRowFlushIfNeed(newRow);
    }

    @Override
    public int getRowCount() {
        return rowNumber;
    }


    private synchronized void addNewRowFlushIfNeed(Row newRow) {
        newRows.add(newRow);
        if (newRows.size() >= QUERY_FLUSH_THRESHOLD) {
            flushNewRows();
        }
    }

    private synchronized void flushNewRows() {
        try {
            TableMate table = toTableMate(prepared.getTable());
            if (newRows.isEmpty()) {
                return;
            }
            prepareInsert(table, newRows);
            affectRows += invokeUpdateWorker(workers);
        } finally {
            newRows.clear();
        }

    }

    @Override
    protected String doExplain() {
        TableMate table = toTableMate(prepared.getTable());
        if (workers != null) {
            return explainForWorker(workers);
        } else {
            Query query = prepared.getQuery();
            String subPlan = query.explainPlan();
            StringBuilder explain = new StringBuilder();
            explain.append("insert into ").append(table.getName()).append(" with query result");
            explain.append(StringUtils.indent(subPlan, 4, false));
            return explain.toString();
        }

    }

}
