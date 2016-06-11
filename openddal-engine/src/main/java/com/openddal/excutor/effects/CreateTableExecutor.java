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

package com.openddal.excutor.effects;

import java.util.ArrayList;
import java.util.List;

import com.openddal.command.CommandInterface;
import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DefineCommand;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Query;
import com.openddal.command.expression.Expression;
import com.openddal.command.expression.Parameter;
import com.openddal.config.TableRule;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableMate;
import com.openddal.engine.Session;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.excutor.handle.UpdateHandler;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.value.DataType;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateTableExecutor extends ExecutionFramework {

    private CreateTable prepared;
    private List<UpdateHandler> updateHandlers;
    private Insert asQueryInsert;
    /**
     * @param prepared
     */
    public CreateTableExecutor(Session session, CreateTable prepared) {
        super(session);
        this.prepared = prepared;
    }

    @Override
    public void doPrepare() {
        String tableName = prepared.getTableName();
        TableMate tableMate = getTableMate(tableName);
        if (tableMate == null) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, tableName);
        }
        TableRule tableRule = tableMate.getTableRule();
        if (!tableMate.isInited()) {
            if (prepared.isIfNotExists()) {
                return;
            }
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_ALREADY_EXISTS_1, tableName);
        }
        TableMate refTable = null;
        for (DefineCommand command : prepared.getConstraintCommands()) {
            if (command.getType() == CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL) {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint) command;
                String refTableName = stmt.getRefTableName();
                refTable = getTableMate(refTableName);
                TableRule r2 = refTable.getTableRule();
                if (refTable != null) {
                    if (!tableRule.isNodeComparable(r2)) {
                        throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                                "Create foreign key for table,the original table and the reference table nodes should be consistency.");
                    }

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

        routingResult = routingHandler.doRoute(tableMate);
        ObjectNode[] selectNodes = routingResult.getSelectNodes();
        updateHandlers = New.arrayList(selectNodes.length);
        for (ObjectNode objectNode : selectNodes) {
            ObjectNode refTableNode = null;
            if (refTable != null) {
                refTableNode = getConsistencyNode(refTable.getTableRule(), objectNode);
            }
            UpdateHandler handler = queryHandlerFactory.createUpdateHandler(prepared, objectNode, refTableNode);
            updateHandlers.add(handler);
        }
    }

    @Override
    public int update() {
        checkPrepared();
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
        int affectRows = executeUpdateHandlers(updateHandlers);

        if (asQueryInsert != null) {
            asQueryInsert.update();
        }
        tableMate.loadMataData(session);
        return affectRows;
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

    @Override
    protected List<Parameter> getPreparedParameters() {
        return prepared.getParameters();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.excutor.ExecutionFramework#isQuery()
     */
    @Override
    protected boolean isQuery() {
        // TODO Auto-generated method stub
        return false;
    }

}
