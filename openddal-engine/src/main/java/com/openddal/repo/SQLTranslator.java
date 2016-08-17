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
package com.openddal.repo;

import java.util.Map;

import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.command.ddl.CreateIndex;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DropIndex;
import com.openddal.command.ddl.DropTable;
import com.openddal.command.ddl.TruncateTable;
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
import com.openddal.route.rule.GroupObjectNode;
import com.openddal.route.rule.ObjectNode;

/**
 * @author jorgie.li
 *
 */
public interface SQLTranslator {

    String identifier(String identifier);

    SQLTranslated translate(Select select, ObjectNode executionOn,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes, Expression[] exprList, Integer limit,
            Integer offset);

    SQLTranslated translate(Select prepared, GroupObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes, Expression[] selectCols, Integer limit,
            Integer offset);

    SQLTranslated translate(Select prepared, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes);

    SQLTranslated translate(CreateTable prepared, ObjectNode node, ObjectNode refNode);

    SQLTranslated translate(DropTable prepared, ObjectNode node);

    SQLTranslated translate(TruncateTable prepared, ObjectNode node);

    SQLTranslated translate(AlterTableAlterColumn prepared, ObjectNode node);

    SQLTranslated translate(AlterTableAddConstraint prepared, ObjectNode node, ObjectNode refNode);

    SQLTranslated translate(CreateIndex prepared, ObjectNode indexNode, ObjectNode tableNode);

    SQLTranslated translate(DropIndex prepared, ObjectNode indexNode, ObjectNode tableNode);

    SQLTranslated translate(Delete delete, ObjectNode node);

    SQLTranslated translate(Insert insert, ObjectNode node, Row... rows);

    SQLTranslated translate(Replace replace, ObjectNode node, Row... rows);

    SQLTranslated translate(Merge merge, ObjectNode node, Row... rows);

    SQLTranslated translate(Update update, ObjectNode node, Row row);

    SQLTranslated translate(Column[] searchColumns, TableFilter filter, ObjectNode node);

    SQLTranslated translate(Column[] searchColumns, TableFilter filter, GroupObjectNode node);

}
