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
package com.openddal.excutor;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import com.openddal.engine.Constants;
import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.route.RoutingHandler;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public abstract class ExecutionFramework {

    protected final Session session;
    protected final Database database;
    protected final ThreadPoolExecutor workExecutor;
    protected final RoutingHandler routingHandler;
    
    protected List<QueryRunner<?>> queryRunners;

    private ObjectNode[] selectNodes;
    
    private boolean prepared;

    /**
     * @param prepared
     */
    public ExecutionFramework(Session session) {
        this.session = session;
        this.database = session.getDatabase();
        this.workExecutor = database.getQueryExecutor();
        this.routingHandler = database.getRoutingHandler();

    }

    public final void prepare() {
        if (prepared) {
            return;
        }
        RoutingResult routingResult = doRoute();
        selectNodes = routingResult.getSelectNodes();
        if(session.getDatabase().getSettings().optimizeMerging) {
            selectNodes = routingResult.group();
        }
        
        prepared = true; 
    }

    protected abstract RoutingResult doRoute();    
    
    public double getCost() {
        checkPrepared();
        return selectNodes.length * Constants.COST_ROW_OFFSET;
    }
    
    public void checkPrepared() {
        if(!prepared) {
            DbException.throwInternalError("Not prepared.");
        }
    }
    
    
}
