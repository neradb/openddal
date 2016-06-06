package com.openddal.repo;

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
import com.openddal.repo.SQLTranslator.Result;
import com.openddal.repo.handle.JdbcQueryHandler;
import com.openddal.route.rule.ObjectNode;

public class JdbcQueryHandlerFactory implements QueryHandlerFactory {

    private final JdbcRepository repo;

    public JdbcQueryHandlerFactory(JdbcRepository repo) {
        this.repo = repo;
    }

    @Override
    public QueryHandler createQueryHandler(Select select, ObjectNode node) {
        Result reslt = repo.getSQLTranslator().translate(select, node);
        JdbcQueryHandler handler = new JdbcQueryHandler(select.getSession(), node.getShardName(), reslt.sql,
                reslt.params);
        return handler;
    }

    @Override
    public QueryHandler createQueryHandler(Select select, ObjectNode node, int limit, int offset) {
        return null;
    }

    @Override
    public QueryHandler createQueryHandler(TableFilter filter, ObjectNode node) {
        return null;
    }

    @Override
    public QueryHandler createQueryHandler(Call call, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(Insert insert, ObjectNode node) {
        return null;
    }

    @Override
    public BatchUpdateHandler createBatchUpdateHandler(Insert insert, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(Update update, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(Delete delete, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(Replace replace, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(Merge merge, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(Call call, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(CreateTable createTable, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(DropTable dropTable, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(TruncateTable truncateTable, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableRename alterTableRename, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableDropConstraint alterTableRename, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(CreateIndex createIndex, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateHandler createUpdateHandler(DropIndex dropIndex, ObjectNode node) {
        return null;
    }

}
