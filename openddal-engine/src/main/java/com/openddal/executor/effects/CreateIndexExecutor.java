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

import com.openddal.command.ddl.CreateIndex;
import com.openddal.dbobject.table.IndexColumn;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class CreateIndexExecutor extends ExecutionFramework {

    private List<UpdateWorker> workers;
    private CreateIndex prepared;

    /**
     * @param session
     * @param prepared
     */
    public CreateIndexExecutor(CreateIndex prepared) {
        this.prepared = prepared;
    }

    @Override
    protected void doPrepare() {
        String tableName = prepared.getTableName();
        String indexName = prepared.getIndexName();
        TableMate table = getTableMate(tableName);
        IndexColumn.mapColumns(prepared.getIndexColumns(), table);
        RoutingResult rr = routingHandler.doRoute(table);
        ObjectNode[] selectNodes = rr.getSelectNodes();
        workers = New.arrayList(selectNodes.length);
        for (ObjectNode tableNode : selectNodes) {
            ObjectNode indexNode = new ObjectNode(tableNode.getShardName(), tableNode.getCatalog(),
                    tableNode.getSchema(), indexName, tableNode.getSuffix());
            UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, indexNode, tableNode);
            workers.add(worker);
        }
    }

    
    
    @Override
    public int doUpdate() {
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        int affectRows = invokeUpdateWorker(workers);
        table.loadMataData(session);
        return affectRows;
    
    }

    @Override
    protected String doExplain() {
        return explainForWorker(workers);
    }

}
