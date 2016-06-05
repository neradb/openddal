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
package com.openddal.repo.mysql;

import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.command.ddl.AlterTableDropConstraint;
import com.openddal.command.ddl.AlterTableRename;
import com.openddal.command.ddl.CreateIndex;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DropIndex;
import com.openddal.command.ddl.DropTable;
import com.openddal.command.ddl.TruncateTable;
import com.openddal.command.dml.Call;
import com.openddal.command.dml.Delete;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Merge;
import com.openddal.command.dml.Replace;
import com.openddal.command.dml.Select;
import com.openddal.command.dml.Update;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.excutor.handle.BatchUpdateHandler;
import com.openddal.excutor.handle.QueryHandler;
import com.openddal.excutor.handle.QueryHandlerFactory;
import com.openddal.excutor.handle.UpdateHandler;
import com.openddal.route.rule.ObjectNode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLHandlerFactory implements QueryHandlerFactory {

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createQueryHandler(com.openddal.command.dml.Select, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public QueryHandler createQueryHandler(Select select, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createQueryHandler(com.openddal.dbobject.table.TableFilter, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public QueryHandler createQueryHandler(TableFilter filter, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createQueryHandler(com.openddal.command.dml.Call, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public QueryHandler createQueryHandler(Call call, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.dml.Insert, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(Insert insert, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createBatchUpdateHandler(com.openddal.command.dml.Insert, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public BatchUpdateHandler createBatchUpdateHandler(Insert insert, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.dml.Update, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(Update update, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.dml.Delete, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(Delete delete, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.dml.Replace, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(Replace replace, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.dml.Merge, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(Merge merge, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.dml.Call, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(Call call, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.CreateTable, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(CreateTable createTable, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.DropTable, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(DropTable dropTable, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.AlterTableAddConstraint, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(AlterTableAddConstraint alterTableAddConstraint, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.AlterTableAlterColumn, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(AlterTableAlterColumn alterTableAlterColumn, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.AlterTableRename, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(AlterTableRename alterTableRename, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.AlterTableDropConstraint, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(AlterTableDropConstraint alterTableRename, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.CreateIndex, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(CreateIndex createIndex, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.openddal.command.ddl.DropIndex, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(DropIndex dropIndex, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.openddal.excutor.handle.QueryHandlerFactory#createUpdateHandler(com.
     * openddal.command.ddl.TruncateTable, com.openddal.route.rule.ObjectNode)
     */
    @Override
    public UpdateHandler createUpdateHandler(TruncateTable truncateTable, ObjectNode executionOn) {
        // TODO Auto-generated method stub
        return null;
    }

}
