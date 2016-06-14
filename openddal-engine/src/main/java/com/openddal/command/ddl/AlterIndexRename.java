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
import com.openddal.dbobject.index.Index;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;

/**
 * This class represents the statement
 * ALTER INDEX RENAME
 */
public class AlterIndexRename extends DefineCommand {

    private Index oldIndex;
    private String newIndexName;

    public AlterIndexRename(Session session) {
        super(session);
    }

    public void setNewName(String name) {
        newIndexName = name;
    }

    @Override
    public int update() {
        throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1, "AlterIndexRename");
    }

    @Override
    public int getType() {
        return CommandInterface.ALTER_INDEX_RENAME;
    }

    public Index getOldIndex() {
        return oldIndex;
    }

    public void setOldIndex(Index index) {
        oldIndex = index;
    }

    public String getNewIndexName() {
        return newIndexName;
    }


}
