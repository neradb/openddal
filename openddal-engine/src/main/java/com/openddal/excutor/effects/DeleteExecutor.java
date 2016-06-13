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
package com.openddal.excutor.effects;

import java.util.ArrayList;
import java.util.List;

import com.openddal.command.dml.Delete;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.excutor.ExecutionFramework;
import com.openddal.excutor.works.UpdateWorker;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class DeleteExecutor extends ExecutionFramework<Delete> {
    
    private List<UpdateWorker> workers;
    /**
     * @param prepared
     */
    public DeleteExecutor(Delete prepared) {
        super(prepared);
    }

    @Override
    public void doPrepare() {
        TableFilter tableFilter = prepared.getTableFilter();
        TableMate table = getTableMate(tableFilter);
        table.check();
        ArrayList<IndexCondition> where = tableFilter.getIndexConditions();
        RoutingResult rr = routingHandler.doRoute(session, table, where);
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
