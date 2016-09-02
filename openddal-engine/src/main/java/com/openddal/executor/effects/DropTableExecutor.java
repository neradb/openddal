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
import java.util.Map;

import com.openddal.command.ddl.DropTable;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

/**
 * @author jorgie.li
 */
public class DropTableExecutor extends ExecutionFramework {

    private Map<DropTable, List<UpdateWorker>> dropWorkers = New.hashMap();
    private DropTable prepared;

    public DropTableExecutor(DropTable prepared) {
        this.prepared = prepared;
    }

    @Override
    protected void doPrepare() {
        prepareDrop(prepared);
    }

    @Override
    protected int doUpdate() {
        executeDrop(prepared);
        return 0;
    }

    private void prepareDrop(DropTable next) {
        String tableName = next.getTableName();
        TableMate table = getTableMate(tableName);
        RoutingResult rr = routingHandler.doRoute(table);
        ObjectNode[] selectNodes = rr.getSelectNodes();
        List<UpdateWorker> workers = New.arrayList(selectNodes.length);
        for (ObjectNode objectNode : selectNodes) {
            UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, objectNode);
            workers.add(worker);
        }
        dropWorkers.put(next, workers);
        next = next.getNext();
        if (next != null) {
            prepareDrop(next);
        }
    }

    private void executeDrop(DropTable next) {
        String tableName = next.getTableName();
        TableMate table = getTableMate(tableName);
        invokeUpdateWorker(dropWorkers.get(next));
        table.markDeleted();
        next = next.getNext();
        if (next != null) {
            executeDrop(next);
        }
    }

    @Override
    protected String doExplain() {
        return executeExplain(prepared);
    }

    private String executeExplain(DropTable next) {
        StringBuilder explain = new StringBuilder();
        String plan = explainForWorker(dropWorkers.get(next));
        explain.append(plan);
        next = next.getNext();
        if (next != null) {
            explain.append("\n");
            explain.append(StringUtils.indent(executeExplain(next), 4, false));
        }
        return explain.toString();
    }

}
