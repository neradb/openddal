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

import java.util.List;

import com.openddal.command.CommandInterface;
import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class AlterTableAddConstraintExecutor extends ExecutionFramework {
    private AlterTableAddConstraint prepared;
    private List<UpdateWorker> workers;


    public AlterTableAddConstraintExecutor(AlterTableAddConstraint prepared) {
        this.prepared = prepared;
    }

    @Override
    public void doPrepare() {
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        TableMate refTable = null;
        int type = prepared.getType();
        switch (type) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
            String refTableName = prepared.getRefTableName();
            refTable = getTableMate(refTableName);
            if (isConsistencyNodeForReferential(table, refTable)) {
                throw DbException.get(ErrorCode.CHECK_CONSTRAINT_INVALID,
                        "Create foreign key for table,the original table and the reference table nodes should be consistency.");
            }
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
            RoutingResult rr = routingHandler.doRoute(table);
            ObjectNode[] selectNodes = rr.getSelectNodes();
            workers = New.arrayList(selectNodes.length);
            for (ObjectNode objectNode : selectNodes) {
                ObjectNode refTableNode = null;
                if (refTable != null) {
                    refTableNode = getConsistencyNode(refTable.getTableRule(), objectNode);
                }
                UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, objectNode, refTableNode);
                workers.add(worker);
            }
            break;
        }
        default:
            throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    public int doUpdate() {
        String tableName = prepared.getTableName();
        TableMate tableMate = getTableMate(tableName);
        int affectRows = invokeUpdateWorker(workers);
        tableMate.loadMataData(session);
        return affectRows;
    }

    @Override
    protected String doExplain() {
        return explainForWorker(workers);
    }

}
