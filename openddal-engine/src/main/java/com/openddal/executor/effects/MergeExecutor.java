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
import com.openddal.command.dml.Merge;
import com.openddal.command.dml.Query;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.message.DbException;
import com.openddal.result.ResultInterface;
import com.openddal.result.Row;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class MergeExecutor extends ExecutionFramework{


    private static final int QUERY_FLUSH_THRESHOLD = 200;
    private int rowNumber;
    private int affectRows;
    private List<Row> mergeRows = New.arrayList(10);
    private List<UpdateWorker> workers;
    private Merge prepared;

    /**
     * @param prepared
     */
    public MergeExecutor(Merge prepared) {
        this.prepared = prepared;
    }

    @Override
    protected void doPrepare() {
        TableMate table = toTableMate(prepared.getTable());
        table.check();
        prepared.setCurrentRowNumber(0);
        ArrayList<Expression[]> list = prepared.getList();
        Column[] columns = prepared.getColumns();
        if (list.size() > 0) {
            List<Row> values = New.arrayList(10);
            for (int x = 0, size = list.size(); x < size; x++) {
                prepared.setCurrentRowNumber(x + 1);
                Expression[] expr = list.get(x);
                Row newRow = table.getTemplateRow();
                for (int i = 0, len = columns.length; i < len; i++) {
                    Column c = columns[i];
                    int index = c.getColumnId();
                    Expression e = expr[i];
                    if (e != null) {
                        // e can be null (DEFAULT)
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
            prepareMerge(table, values);
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
            rowNumber = 0;
            affectRows = 0;
            TableMate table = toTableMate(prepared.getTable());
            Query query = prepared.getQuery();
            Column[] columns = prepared.getColumns();
            ResultInterface rows = query.query(0);
            while (rows.next()) {
                rowNumber++;
                Value[] r = rows.currentRow();
                Row newRow = table.getTemplateRow();
                prepared.setCurrentRowNumber(rowNumber);
                for (int j = 0; j < columns.length; j++) {
                    Column c = columns[j];
                    int index = c.getColumnId();
                    try {
                        Value v = c.convert(r[j]);
                        newRow.setValue(index, v);
                    } catch (DbException ex) {
                        throw prepared.setRow(ex, rowNumber, Prepared.getSQL(r));
                    }
                }
                addMergeRowFlushIfNeed(newRow);
            }
            flushMergeRows();
            rows.close();
            return affectRows;
        }

    }

    private void prepareMerge(TableMate table, List<Row> rows) {
        session.checkCanceled();
        Map<ObjectNode, List<Row>> batches = batchForRoutingNode(table, rows);
        workers = New.arrayList(batches.size());
        for (Map.Entry<ObjectNode, List<Row>> item : batches.entrySet()) {
            Row[] values = item.getValue().toArray(new Row[item.getValue().size()]);
            UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, item.getKey(), values);
            workers.add(worker);
        }
    }


    private synchronized void addMergeRowFlushIfNeed(Row replace) {
        mergeRows.add(replace);
        if (mergeRows.size() >= QUERY_FLUSH_THRESHOLD) {
            flushMergeRows();
        }
    }

    private synchronized void flushMergeRows() {
        try {
            TableMate table = toTableMate(prepared.getTable());
            if (mergeRows.isEmpty()) {
                return;
            }
            prepareMerge(table, mergeRows);
            affectRows += invokeUpdateWorker(workers);
        } finally {
            mergeRows.clear();
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
            explain.append("merge into ").append(table.getName()).append(" with query result");
            explain.append(StringUtils.indent(subPlan, 4, false));
            return explain.toString();
        }

    }



}
