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

import com.openddal.command.ddl.TruncateTable;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class TruncateTableExecutor extends ExecutionFramework {

    private List<UpdateWorker> workers;
    private TruncateTable prepared;

    /**
     * @param prepared
     */
    public TruncateTableExecutor(TruncateTable prepared) {
        this.prepared = prepared;
    }

    @Override
    protected void doPrepare() {
        TableMate table = getTableMate(prepared.getTable().getName());
        RoutingResult rr = routingHandler.doRoute(table);
        ObjectNode[] selectNodes = rr.getSelectNodes();
        workers = New.arrayList(selectNodes.length);
        for (ObjectNode objectNode : selectNodes) {
            UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, objectNode);
            workers.add(worker);
        }
    }

    @Override
    public int doUpdate() {
        return invokeUpdateWorker(workers);

    }


    @Override
    protected String doExplain() {
        return explainForWorker(workers);
    }

}
