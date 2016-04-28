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
package com.openddal.excutor.dml;

import java.util.ArrayList;
import java.util.List;

import com.openddal.command.dml.Delete;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.result.SearchRow;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StatementBuilder;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DeleteExecutor extends PreparedRoutingExecutor<Delete> {

    /**
     * @param prepared
     */
    public DeleteExecutor(Delete prepared) {
        super(prepared);
    }

    @Override
    public int executeUpdate() {
        TableFilter tableFilter = prepared.getTableFilter();
        TableMate table = castTableMate(tableFilter.getTable());
        table.check();
        return updateRow(table, null, tableFilter.getIndexConditions());
    }

    @Override
    protected List<Value> doTranslate(ObjectNode node, SearchRow row, StatementBuilder buff) {
        ArrayList<Value> params = New.arrayList();
        TableFilter tableFilter = prepared.getTableFilter();
        String forTable = node.getCompositeObjectName();
        Expression condition = prepared.getCondition();
        Expression limitExpr = prepared.getLimitExpr();

        buff.append("DELETE FROM ");
        buff.append(identifier(forTable));
        if (condition != null) {
            condition.exportParameters(tableFilter, params);
            buff.append(" WHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (limitExpr != null) {
            limitExpr.exportParameters(tableFilter, params);
            buff.append(" LIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
        }
        return params;

    }

}
