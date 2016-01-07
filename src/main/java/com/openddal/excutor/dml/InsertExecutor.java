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
package com.openddal.excutor.dml;

import com.openddal.command.Prepared;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Query;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.Right;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.message.DbException;
import com.openddal.result.ResultInterface;
import com.openddal.result.ResultTarget;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.TableNode;
import com.openddal.util.New;
import com.openddal.util.StatementBuilder;
import com.openddal.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *         TODO validation rule column
 */
public class InsertExecutor extends PreparedRoutingExecutor<Insert> implements ResultTarget {

    private int rowNumber;
    private int affectRows;
    private List<Row> newRows = New.arrayList(10);

    /**
     * @param prepared
     */
    public InsertExecutor(Insert prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        TableMate table = castTableMate(prepared.getTable());
        table.check();
        session.getUser().checkRight(table, Right.INSERT);
        prepared.setCurrentRowNumber(0);
        rowNumber = 0;
        affectRows = 0;
        ArrayList<Expression[]> list = prepared.getList();
        Column[] columns = prepared.getColumns();
        int listSize = list.size();
        if (listSize > 0) {
            int columnLen = columns.length;
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
                rowNumber++;
                table.validateConvertUpdateSequence(session, newRow);
                addNewRow(newRow);

            }
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
        }
        flushNewRows();
        return affectRows;

    }

    @Override
    public void addRow(Value[] values) {
        TableMate table = castTableMate(prepared.getTable());
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
        table.validateConvertUpdateSequence(session, newRow);
        addNewRow(newRow);
    }

    @Override
    public int getRowCount() {
        return rowNumber;
    }

    private void addNewRow(Row newRow) {
        newRows.add(newRow);
        if (newRows.size() >= 200) {
            flushNewRows();
        }
    }

    private void flushNewRows() {
        try {
            TableMate table = castTableMate(prepared.getTable());
            if (newRows.isEmpty()) {
                return;
            } else if (newRows.size() == 1) {
                affectRows += updateRow(table, newRows.get(0));
            } else {
                affectRows += updateRows(table, newRows);
            }
        } finally {
            newRows.clear();
        }

    }

    @Override
    protected List<Value> doTranslate(TableNode node, SearchRow row, StatementBuilder buff) {
        String forTable = node.getCompositeObjectName();
        TableMate table = castTableMate(prepared.getTable());
        Column[] columns = table.getColumns();
        return buildInsert(forTable, columns, row, buff);
    }

}
