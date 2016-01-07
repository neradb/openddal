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
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Session;
import com.openddal.message.DbException;

/**
 * This class represents the statement
 * ALTER TABLE DROP CONSTRAINT
 */
public class AlterTableDropConstraint extends SchemaCommand {

    private final boolean ifExists;
    private String constraintName;

    public AlterTableDropConstraint(Session session, Schema schema,
                                    boolean ifExists) {
        super(session, schema);
        this.ifExists = ifExists;
    }

    @Override
    public int update() {
        throw DbException.getUnsupportedException("TODO");
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_TABLE_DROP_CONSTRAINT;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getConstraintName() {
        return constraintName;
    }

    public void setConstraintName(String string) {
        constraintName = string;
    }


}
