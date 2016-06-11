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
package com.openddal.excutor.handle;

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
import com.openddal.dbobject.table.TableFilter;
import com.openddal.route.rule.ObjectNode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public interface QueryHandlerFactory {
    
    QueryHandler createQueryHandler(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes);

    QueryHandler createQueryHandler(TableFilter filter, ObjectNode node);

    QueryHandler createQueryHandler(Call call, ObjectNode node);

    UpdateHandler createUpdateHandler(Insert insert, ObjectNode node);

    BatchUpdateHandler createBatchUpdateHandler(Insert insert, ObjectNode node);

    UpdateHandler createUpdateHandler(Update update, ObjectNode node);

    UpdateHandler createUpdateHandler(Delete delete, ObjectNode node);

    UpdateHandler createUpdateHandler(Replace replace, ObjectNode node);

    UpdateHandler createUpdateHandler(Merge merge, ObjectNode node);

    UpdateHandler createUpdateHandler(Call call, ObjectNode node);

    UpdateHandler createUpdateHandler(CreateTable createTable, ObjectNode node, ObjectNode refNode);

    UpdateHandler createUpdateHandler(DropTable dropTable, ObjectNode node);

    UpdateHandler createUpdateHandler(TruncateTable truncateTable, ObjectNode node);

    UpdateHandler createUpdateHandler(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node);

    UpdateHandler createUpdateHandler(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node);

    UpdateHandler createUpdateHandler(AlterTableRename alterTableRename, ObjectNode node);

    UpdateHandler createUpdateHandler(AlterTableDropConstraint alterTableRename, ObjectNode node);

    UpdateHandler createUpdateHandler(CreateIndex createIndex, ObjectNode node);

    UpdateHandler createUpdateHandler(DropIndex dropIndex, ObjectNode node);

}
