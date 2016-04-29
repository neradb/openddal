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
package com.openddal.dbobject;

import com.openddal.command.Parser;
import com.openddal.engine.Database;
import com.openddal.message.Trace;

/**
 * A database object such as a table, an index, or a user.
 */
public abstract class DbObject {

    /**
     * The object is of the type table or view.
     */
    public static final int TABLE_OR_VIEW = 0;

    /**
     * This object is an index.
     */
    public static final int INDEX = 1;

    /**
     * This object is a user.
     */
    public static final int USER = 2;

    /**
     * This object is a sequence.
     */
    public static final int SEQUENCE = 3;

    /**
     * This object is a schema.
     */
    public static final int SCHEMA = 4;
    
    /**
     * The database.
     */
    protected Database database;
    protected Trace trace;

    private int id;
    private String objectName;
    private boolean temporary;

    /**
     * Initialize some attributes of this object.
     *
     * @param db          the database
     * @param objectId    the object id
     * @param name        the name
     * @param traceModule the trace module name
     */
    protected void initDbObjectBase(Database db, String name) {
        this.database = db;
        this.id = db.allocateObjectId();
        this.objectName = name;
        this.trace = database.getTrace(Trace.DATABASE);
    }

    protected void setObjectName(String name) {
        objectName = name;
    }

    public String getSQL() {
        return Parser.quoteIdentifier(objectName);
    }

    public Database getDatabase() {
        return database;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return objectName;
    }
    
    public boolean isTemporary() {
        return temporary;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }
    
    public abstract int getType();
    
    public abstract void checkRename();
    

    public void rename(String newName) {
        checkRename();
        objectName = newName;
    }

    
    @Override
    public String toString() {
        return "DbObject [database=" + database + ", id=" + id + ", objectName=" + objectName + ", type=" + getType() + "]";
    }    
    
}
