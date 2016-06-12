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

import com.openddal.command.CommandInterface;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.Table;
import com.openddal.dbobject.table.TableMate;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.util.StatementBuilder;

/**
 * This executor execute the statements
 * ALTER TABLE ADD,
 * ALTER TABLE ADD IF NOT EXISTS,
 * ALTER TABLE ALTER COLUMN,
 * ALTER TABLE ALTER COLUMN RESTART,
 * ALTER TABLE ALTER COLUMN SELECTIVITY,
 * ALTER TABLE ALTER COLUMN SET DEFAULT,
 * ALTER TABLE ALTER COLUMN SET NOT NULL,
 * ALTER TABLE ALTER COLUMN SET NULL,
 * ALTER TABLE DROP COLUMN
 *
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class AlterTableAlterColumnExecutor extends ExecutionFramework<AlterTableAlterColumn> {
    
    public AlterTableAlterColumnExecutor(AlterTableAlterColumn prepared) {
        super(prepared);
    }

    @Override
    public int doUpdate() {
        Table parseTable = prepared.getTable();
        if (!(parseTable instanceof TableMate)) {
            DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, parseTable.getSQL());
        }
        TableMate table = (TableMate) parseTable;
        session.getUser().checkRight(table, Right.ALL);
        TableNode[] tableNodes = table.getPartitionNode();
        Column oldColumn = prepared.getOldColumn();
        int type = prepared.getType();
        switch (type) {
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
                if (!oldColumn.isNullable()) {
                    break;
                }
            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL: {
                if (oldColumn.isNullable()) {
                    break;
                }
            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT:
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE:
            case CommandInterface.ALTER_TABLE_ADD_COLUMN:
            case CommandInterface.ALTER_TABLE_DROP_COLUMN: {
                execute(tableNodes);
                table.loadMataData(session);
                break;
            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY: {
                return 0;//not supported.
            }
            default:
                throw DbException.throwInternalError("type=" + type);
        }
        return 0;
    }


    @Override
    protected String doTranslate(TableNode node) {
        Column oldColumn = prepared.getOldColumn();
        String forTable = node.getCompositeObjectName();
        int type = prepared.getType();
        switch (type) {
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
                StringBuilder buff = new StringBuilder("ALTER TABLE ");
                buff.append(identifier(forTable));
                buff.append(" CHANGE COLUMN ");
                buff.append(oldColumn.getSQL()).append(' ');
                buff.append(oldColumn.getCreateSQL());
                return buff.toString();
            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL: {

                StringBuilder buff = new StringBuilder("ALTER TABLE ");
                buff.append(identifier(forTable));
                buff.append(" CHANGE COLUMN ");
                buff.append(oldColumn.getSQL()).append(' ');
                buff.append(oldColumn.getCreateSQL());
                return buff.toString();
            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT: {
                StringBuilder buff = new StringBuilder("ALTER TABLE");
                buff.append(identifier(forTable));
                buff.append(" ALTER COLUMN ");
                buff.append(prepared.getOldColumn().getSQL());
                buff.append(" SET DEFAULT ");
                buff.append(prepared.getDefaultExpression().getSQL());
                return buff.toString();
            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: {
                StringBuilder buff = new StringBuilder("ALTER TABLE");
                buff.append(identifier(forTable));
                buff.append(" ALTER COLUMN ");
                buff.append(prepared.getOldColumn().getSQL());
                buff.append(" SET DEFAULT ");
                buff.append(prepared.getDefaultExpression().getSQL());
                return buff.toString();
            }
            case CommandInterface.ALTER_TABLE_ADD_COLUMN: {
                StatementBuilder buff = new StatementBuilder("ALTER TABLE");
                buff.append(identifier(forTable));
                ArrayList<Column> columnsToAdd = getPrepared().getColumnsToAdd();
                for (Column column : columnsToAdd) {
                    buff.appendExceptFirst(", ");
                    buff.append(" ADD COLUMN ");
                    buff.append(column.getCreateSQL());
                }
                return buff.toString();
            }
            case CommandInterface.ALTER_TABLE_DROP_COLUMN: {
                StatementBuilder buff = new StatementBuilder("ALTER TABLE");
                buff.append(identifier(forTable));
                buff.append(" DROP COLUMN ");
                buff.append(oldColumn.getSQL());
                return buff.toString();

            }
            case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY:
            default:
                throw DbException.throwInternalError("type=" + type);
        }
    }

    @Override
    protected void doPrepare() {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected String doExplain() {
        // TODO Auto-generated method stub
        return null;
    }

}
