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

import com.openddal.command.dml.Delete;
import com.openddal.dbobject.index.ConditionExtractor;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class DeleteExecutor extends ExecutionFramework {
    
    private List<UpdateWorker> workers;
    private boolean alwaysFalse;
    private Delete prepared;

    /**
     * @param prepared
     */
    public DeleteExecutor(Delete prepared) {
        this.prepared = prepared;
    }

    @Override
    public void doPrepare() {
        TableFilter tableFilter = prepared.getTableFilter();
        TableMate table = getTableMate(tableFilter);
        ConditionExtractor extractor = new ConditionExtractor(tableFilter);
        alwaysFalse = extractor.isAlwaysFalse();
        if(!alwaysFalse) {
            RoutingResult rr = routingHandler.doRoute(table, 
                    extractor.getStart(),extractor.getEnd(), extractor.getInColumns());
            ObjectNode[] selectNodes = rr.getSelectNodes();
            workers = New.arrayList(selectNodes.length);
            for (ObjectNode objectNode : selectNodes) {
                UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, objectNode);
                workers.add(worker);
            }
        }
        
    }


    
    @Override
    public int doUpdate() {
        if(this.alwaysFalse) {
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
