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
package com.openddal.command.dml;

import java.util.ArrayList;
import java.util.HashMap;

import com.openddal.command.CommandInterface;
import com.openddal.command.Prepared;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.Parameter;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.PlanItem;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.ResultInterface;
import com.openddal.util.New;

/**
 * This class represents the statement
 * UPDATE
 */
public class Update extends Prepared {

    private final ArrayList<Column> columns = New.arrayList();
    private final HashMap<Column, Expression> expressionMap = New.hashMap();
    private Expression condition;
    private TableFilter tableFilter;
    /**
     * The limit expression as specified in the LIMIT clause.
     */
    private Expression limitExpr;

    public Update(Session session) {
        super(session);
    }

    /**
     * Add an assignment of the form column = expression.
     *
     * @param column     the column
     * @param expression the expression
     */
    public void setAssignment(Column column, Expression expression) {
        if (expressionMap.containsKey(column)) {
            throw DbException.get(ErrorCode.DUPLICATE_COLUMN_NAME_1, column
                    .getName());
        }
        columns.add(column);
        expressionMap.put(column, expression);
        if (expression instanceof Parameter) {
            Parameter p = (Parameter) expression;
            p.setColumn(column);
        }
    }

    @Override
    public void prepare() {
        if (condition != null) {
            condition.mapColumns(tableFilter, 0);
            condition = condition.optimize(session);
            condition.createIndexConditions(session, tableFilter);
        }
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = expressionMap.get(c);
            e.mapColumns(tableFilter, 0);
            expressionMap.put(c, e.optimize(session));
        }
        TableFilter[] filters = new TableFilter[] { tableFilter };
        PlanItem item = tableFilter.getBestPlanItem(session, filters, 0);
        tableFilter.setPlanItem(item);
        tableFilter.prepare();
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return CommandInterface.UPDATE;
    }

    public void setLimit(Expression limit) {
        this.limitExpr = limit;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

    //getter
    public ArrayList<Column> getColumns() {
        return columns;
    }

    public HashMap<Column, Expression> getExpressionMap() {
        return expressionMap;
    }

    public Expression getCondition() {
        return condition;
    }

    public void setCondition(Expression condition) {
        this.condition = condition;
    }

    public TableFilter getTableFilter() {
        return tableFilter;
    }

    public void setTableFilter(TableFilter tableFilter) {
        this.tableFilter = tableFilter;
    }

    public Expression getLimitExpr() {
        return limitExpr;
    }
    

}
