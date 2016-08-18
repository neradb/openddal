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
package com.openddal.command.dml;


import com.openddal.command.CommandInterface;
import com.openddal.command.Prepared;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.result.ResultInterface;

/**
 * Represents a transactional statement.
 */
public class TransactionCommand extends Prepared {

    private final int type;
    private String savepointName;
    private String transactionName;

    public TransactionCommand(Session session, int type) {
        super(session);
        this.type = type;
    }

    public void setSavepointName(String name) {
        this.savepointName = name;
    }

    @Override
    public int update() {
        switch (type) {
        case CommandInterface.SET_AUTOCOMMIT_TRUE:
            session.setAutoCommit(true);
            break;
        case CommandInterface.SET_AUTOCOMMIT_FALSE:
            session.setAutoCommit(false);
            break;
        case CommandInterface.BEGIN:
            session.begin();
            break;
        case CommandInterface.COMMIT:
            session.commit();
            break;
        case CommandInterface.ROLLBACK:
            session.rollback();
            break;
        case CommandInterface.SAVEPOINT:
            session.addSavepoint(savepointName);
            break;
        case CommandInterface.ROLLBACK_TO_SAVEPOINT:
            session.rollbackToSavepoint(savepointName);
            break;
        case CommandInterface.RELEASE_SAVEPOINT:
            session.releaseSavepoint(savepointName);
            break;
        default:
            DbException.throwInternalError("type=" + type);
        }
        return 0;
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean needRecompile() {
        return false;
    }

    public void setTransactionName(String string) {
        this.transactionName = string;
    }

    public String getTransactionName() {
        return transactionName;
    }

    @Override
    public ResultInterface queryMeta() {
        return null;
    }

    @Override
    public int getType() {
        return type;
    }

    @Override
    public boolean isCacheable() {
        return true;
    }

}
