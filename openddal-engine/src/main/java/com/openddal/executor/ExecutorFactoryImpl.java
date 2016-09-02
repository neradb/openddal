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
package com.openddal.executor;

import com.openddal.command.CommandInterface;
import com.openddal.command.Prepared;
import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.command.ddl.CreateIndex;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DropIndex;
import com.openddal.command.ddl.DropTable;
import com.openddal.command.ddl.TruncateTable;
import com.openddal.command.dml.Call;
import com.openddal.command.dml.Delete;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Merge;
import com.openddal.command.dml.Replace;
import com.openddal.command.dml.Update;
import com.openddal.executor.effects.AlterTableAddConstraintExecutor;
import com.openddal.executor.effects.AlterTableAlterColumnExecutor;
import com.openddal.executor.effects.CallExecutor;
import com.openddal.executor.effects.CreateIndexExecutor;
import com.openddal.executor.effects.CreateTableExecutor;
import com.openddal.executor.effects.DeleteExecutor;
import com.openddal.executor.effects.DropIndexExecutor;
import com.openddal.executor.effects.DropTableExecutor;
import com.openddal.executor.effects.InsertExecutor;
import com.openddal.executor.effects.MergeExecutor;
import com.openddal.executor.effects.ReplaceExecutor;
import com.openddal.executor.effects.TruncateTableExecutor;
import com.openddal.executor.effects.UpdateExecutor;

/**
 * @author jorgie.li
 *
 */
public class ExecutorFactoryImpl implements ExecutorFactory {

    @Override
    public Executor newExecutor(Prepared prepared) {
        return create(prepared);
    }

    private Executor create(Prepared prepared) {
        int type = prepared.getType();
        switch (type) {
        // ddal
        case CommandInterface.CREATE_TABLE:
            return new CreateTableExecutor((CreateTable) prepared);
        case CommandInterface.DROP_TABLE:
            return new DropTableExecutor((DropTable) prepared);
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE:
        case CommandInterface.ALTER_TABLE_ADD_COLUMN:
        case CommandInterface.ALTER_TABLE_DROP_COLUMN:
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY:
            return new AlterTableAlterColumnExecutor((AlterTableAlterColumn) prepared);
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE:
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK:
            return new AlterTableAddConstraintExecutor((AlterTableAddConstraint) prepared);
        case CommandInterface.TRUNCATE_TABLE:
            return new TruncateTableExecutor((TruncateTable) prepared);
        case CommandInterface.CREATE_INDEX:
            return new CreateIndexExecutor((CreateIndex) prepared);
        case CommandInterface.DROP_INDEX:
            return new DropIndexExecutor((DropIndex) prepared);
        // dml
        case CommandInterface.INSERT:
            return new InsertExecutor((Insert) prepared);
        case CommandInterface.DELETE:
            return new DeleteExecutor((Delete) prepared);
        case CommandInterface.UPDATE:
            return new UpdateExecutor((Update) prepared);
        case CommandInterface.REPLACE:
            return new ReplaceExecutor((Replace) prepared);
        case CommandInterface.MERGE:
            return new MergeExecutor((Merge) prepared);
        case CommandInterface.CALL:
            return new CallExecutor((Call) prepared);
        default:
            return null;
        }
    }

}
