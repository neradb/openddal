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
// Created on 2015年4月12日
// $Id$

package com.openddal.executor.effects;

import java.util.ArrayList;
import java.util.List;

import com.openddal.command.CommandInterface;
import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DefineCommand;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Query;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.DataType;

/**
 * @author jorgie.li
 */
public class CreateTableExecutor extends ExecutionFramework {

    private List<UpdateWorker> workers;
    private Insert asQueryInsert;
    private CreateTable prepared;
    /**
     * @param prepared
     */
    public CreateTableExecutor(CreateTable prepared) {
        this.prepared = prepared;
    }

    @Override
    public void doPrepare() {
        String tableName = prepared.getTableName();
        TableMate tableMate = getTableMate(tableName);
        TableMate refTable = null;
        for (DefineCommand command : prepared.getConstraintCommands()) {
            if (command.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL) {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint) command;
                String refTableName = stmt.getRefTableName();
                refTable = getTableMate(refTableName);
                if (!isConsistencyNodeForReferential(tableMate, refTable)) {
                    throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                            "Create foreign key for table,the original table and the reference table nodes should be consistency.");
                }
            }
        }
        for (Column c : tableMate.getColumns()) {
            c.prepareExpression(session);
        }

        Query query = prepared.getQuery();
        if (query != null) {
            query.prepare();
            if (prepared.getColumnCount() == 0) {
                generateColumnsFromQuery();
            } else if (prepared.getColumnCount() != query.getColumnCount()) {
                throw DbException.get(ErrorCode.COLUMN_COUNT_DOES_NOT_MATCH);
            }
        }

        if (query != null) {
            asQueryInsert = new Insert(session);
            asQueryInsert.setSortedInsertMode(prepared.isSortedInsertMode());
            asQueryInsert.setQuery(query);
            asQueryInsert.setTable(tableMate);
            asQueryInsert.setInsertFromSelect(true);
            asQueryInsert.prepare();
            // asQueryInsert.update();
        }

        RoutingResult rr = routingHandler.doRoute(tableMate);
        ObjectNode[] selectNodes = rr.getSelectNodes();
        workers = New.arrayList(selectNodes.length);
        for (ObjectNode objectNode : selectNodes) {
            ObjectNode refTableNode = null;
            if (refTable != null) {
                refTableNode = getConsistencyNode(refTable.getTableRule(), objectNode);
            }
            UpdateWorker handler = queryHandlerFactory.createUpdateWorker(prepared, objectNode, refTableNode);
            workers.add(handler);
        }
    }

    @Override
    public int doUpdate() {
        String tableName = prepared.getTableName();
        TableMate tableMate = getTableMate(tableName);
        ArrayList<Sequence> sequences = New.arrayList();
        for (Column c : tableMate.getColumns()) {
            if (c.isAutoIncrement()) {
            }
            Sequence seq = c.getSequence();
            if (seq != null) {
                sequences.add(seq);
            }
        }
        for (Sequence sequence : sequences) {
            tableMate.addSequence(sequence);
        }
        int affectRows = invokeUpdateWorker(workers);

        if (asQueryInsert != null) {
            asQueryInsert.update();
        }
        tableMate.loadMataData(session);
        return affectRows;
    }
    
    /**
     * Get the PreparedExecutor with the execution explain.
     *
     * @return the execution explain
     */
    @Override
    public String doExplain() {
        StringBuilder explain = new StringBuilder();
        explain.append(explainForWorker(workers));
        if(asQueryInsert != null) {
            String explainPlan = asQueryInsert.explainPlan();
            explain.append(StringUtils.indent(explainPlan, 4, false));
        }
        return explain.toString();
    
    }

    private void generateColumnsFromQuery() {
        int columnCount = prepared.getQuery().getColumnCount();
        ArrayList<Expression> expressions = prepared.getQuery().getExpressions();
        for (int i = 0; i < columnCount; i++) {
            Expression expr = expressions.get(i);
            int type = expr.getType();
            String name = expr.getAlias();
            long precision = expr.getPrecision();
            int displaySize = expr.getDisplaySize();
            DataType dt = DataType.getDataType(type);
            if (precision > 0 && (dt.defaultPrecision == 0
                    || (dt.defaultPrecision > precision && dt.defaultPrecision < Byte.MAX_VALUE))) {
                // dont' set precision to MAX_VALUE if this is the default
                precision = dt.defaultPrecision;
            }
            int scale = expr.getScale();
            if (scale > 0 && (dt.defaultScale == 0 || (dt.defaultScale > scale && dt.defaultScale < precision))) {
                scale = dt.defaultScale;
            }
            if (scale > precision) {
                precision = scale;
            }
            Column col = new Column(name, type, precision, scale, displaySize);
            prepared.addColumn(col);
        }
    }


}
