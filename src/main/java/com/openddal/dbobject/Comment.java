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
package com.openddal.dbobject;

import com.openddal.engine.Database;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.Trace;

/**
 * Represents a database object comment.
 */
public class Comment extends DbObjectBase {

    private final int objectType;
    private final String objectName;
    private String commentText;

    public Comment(Database database, int id, DbObject obj) {
        initDbObjectBase(database, id, getKey(obj), Trace.DATABASE);
        this.objectType = obj.getType();
        this.objectName = obj.getSQL();
    }

    private static String getTypeName(int type) {
        switch (type) {
            case CONSTANT:
                return "CONSTANT";
            case CONSTRAINT:
                return "CONSTRAINT";
            case FUNCTION_ALIAS:
                return "ALIAS";
            case INDEX:
                return "INDEX";
            case ROLE:
                return "ROLE";
            case SCHEMA:
                return "SCHEMA";
            case SEQUENCE:
                return "SEQUENCE";
            case TABLE_OR_VIEW:
                return "TABLE";
            case TRIGGER:
                return "TRIGGER";
            case USER:
                return "USER";
            case USER_DATATYPE:
                return "DOMAIN";
            default:
                // not supported by parser, but required when trying to find a
                // comment
                return "type" + type;
        }
    }

    /**
     * Get the comment key name for the given database object. This key name is
     * used internally to associate the comment to the object.
     *
     * @param obj the object
     * @return the key name
     */
    public static String getKey(DbObject obj) {
        return getTypeName(obj.getType()) + " " + obj.getSQL();
    }

    @Override
    public int getType() {
        return COMMENT;
    }

    @Override
    public void removeChildrenAndResources(Session session) {
        //do nothing
    }

    @Override
    public void checkRename() {
        DbException.throwInternalError();
    }

    public int getObjectType() {
        return objectType;
    }

    public String getObjectName() {
        return objectName;
    }

    public String getCommentText() {
        return commentText;
    }

    /**
     * Set the comment text.
     *
     * @param comment the text
     */
    public void setCommentText(String comment) {
        this.commentText = comment;
    }


}
