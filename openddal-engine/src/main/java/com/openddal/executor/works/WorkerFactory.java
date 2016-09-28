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
package com.openddal.executor.works;

import java.util.Map;

import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.command.ddl.AlterTableDropConstraint;
import com.openddal.command.ddl.AlterTableRename;
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
import com.openddal.command.dml.Select;
import com.openddal.command.dml.Update;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.result.Row;
import com.openddal.route.rule.ObjectNode;

/**
 * @author jorgie.li
 *
 */
public interface WorkerFactory {
    
    QueryWorker createQueryWorker(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes, Expression[] rewriteCols,
            Integer limit, Integer offset);

    QueryWorker createQueryWorker(Column[] searchColumns, TableFilter filter, ObjectNode node);

    QueryWorker createQueryWorker(Call call, ObjectNode node);

    UpdateWorker createUpdateWorker(Insert insert, ObjectNode node, Row ... rows);

    UpdateWorker createUpdateWorker(Update update, ObjectNode node, Row row);

    UpdateWorker createUpdateWorker(Delete delete, ObjectNode node);

    UpdateWorker createUpdateWorker(Replace replace, ObjectNode node, Row ... rows);

    UpdateWorker createUpdateWorker(Merge merge, ObjectNode node, Row ... rows);

    UpdateWorker createUpdateWorker(Call call, ObjectNode node);

    UpdateWorker createUpdateWorker(CreateTable createTable, ObjectNode node, ObjectNode refNode);

    UpdateWorker createUpdateWorker(DropTable dropTable, ObjectNode node);

    UpdateWorker createUpdateWorker(TruncateTable truncateTable, ObjectNode node);

    UpdateWorker createUpdateWorker(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node, ObjectNode refNode);

    UpdateWorker createUpdateWorker(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node);

    UpdateWorker createUpdateWorker(AlterTableRename alterTableRename, ObjectNode node);

    UpdateWorker createUpdateWorker(AlterTableDropConstraint alterTableRename, ObjectNode node);

    UpdateWorker createUpdateWorker(CreateIndex createIndex, ObjectNode indexNode, ObjectNode tableNode);

    UpdateWorker createUpdateWorker(DropIndex dropIndex, ObjectNode indexNode, ObjectNode tableNode);
    
}
