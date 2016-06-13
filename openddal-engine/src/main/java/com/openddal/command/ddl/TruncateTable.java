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
package com.openddal.command.ddl;

import com.openddal.command.CommandInterface;
import com.openddal.dbobject.table.Table;
import com.openddal.engine.Session;
import com.openddal.excutor.effects.TruncateTableExecutor;

/**
 * This class represents the statement
 * TRUNCATE TABLE
 */
public class TruncateTable extends DefineCommand {

    private Table table;
    private TruncateTableExecutor executor;

    public TruncateTable(Session session) {
        super(session);
    }

    @Override
    public int getType() {
        return CommandInterface.TRUNCATE_TABLE;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    @Override
    public TruncateTableExecutor getExecutor() {
        if(executor == null) {
            executor = new TruncateTableExecutor(this);
        }
        return executor;
    }
}
