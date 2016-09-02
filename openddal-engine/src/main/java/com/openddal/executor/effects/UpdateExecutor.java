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
package com.openddal.executor.effects;

import java.util.List;
import java.util.Map;

import com.openddal.command.dml.Update;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.index.ConditionExtractor;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.result.Row;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.value.Value;

/**
 * @author jorgie.li
 */
public class UpdateExecutor extends ExecutionFramework {

    private List<UpdateWorker> workers;
    private boolean alwaysFalse;
    private Update prepared;

    /**
     * @param prepared
     */
    public UpdateExecutor(Update prepared) {
        this.prepared = prepared;
    }

    
    @Override
    protected void doPrepare() {
        TableFilter tableFilter = prepared.getTableFilter();
        TableMate table = getTableMate(tableFilter);
        table.check();
        List<Column> columns = prepared.getColumns();
        Map<Column, Expression> valueMap = prepared.getExpressionMap();
        Column[] ruleColumns = table.getRuleColumns();
        for (Column column : ruleColumns) {
            if(valueMap.get(column) != null) {
                throw DbException.get(ErrorCode.SHARDING_COLUMNS_CANNOT_BE_MODIFIED, column.getName());
            }
        }
        Row updateRow = table.getTemplateRow();
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            Expression e = valueMap.get(c);
            int index = c.getColumnId();
            if (e != null) {
                // e can be null (DEFAULT)
                e = e.optimize(session);
                try {
                    Value v = c.convert(e.getValue(session));
                    updateRow.setValue(index, v);
                } catch (DbException ex) {
                    ex.addSQL("evaluate expression " + e.getSQL());
                    throw ex;
                }
            }
        }
        ConditionExtractor extractor = new ConditionExtractor(tableFilter);
        alwaysFalse = extractor.isAlwaysFalse();
        if(!alwaysFalse) {
            RoutingResult rr = routingHandler.doRoute(table, 
                    extractor.getStart(),extractor.getEnd(), extractor.getInColumns());
            ObjectNode[] selectNodes = rr.getSelectNodes();
            workers = New.arrayList(selectNodes.length);
            for (ObjectNode objectNode : selectNodes) {
                UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, objectNode, updateRow);
                workers.add(worker);
            }
        }
        
    }

    @Override
    public int doUpdate() {
        if(alwaysFalse) {
           return 0; 
        }
        return invokeUpdateWorker(workers);
    }


    @Override
    protected String doExplain() {
        if(this.alwaysFalse) {
            return "always false statement";
        }
        return explainForWorker(workers);
    }



}
