package com.openddal.repo;

import java.util.Map;

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
import com.openddal.excutor.works.BatchUpdateWorker;
import com.openddal.excutor.works.QueryWorker;
import com.openddal.excutor.works.UpdateWorker;
import com.openddal.excutor.works.WorkerFactory;
import com.openddal.repo.works.JdbcQueryWorker;
import com.openddal.repo.works.JdbcUpdateWorker;
import com.openddal.route.rule.ObjectNode;

public class JdbcWorkerFactory implements WorkerFactory {

    private final JdbcRepository repo;

    public JdbcWorkerFactory(JdbcRepository repo) {
        this.repo = repo;
    }

    @Override
    public QueryWorker createQueryWorker(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes) {
        SQLTranslated translated = repo.getSQLTranslator().translate(select, node, consistencyTableNodes);
        JdbcQueryWorker handler = new JdbcQueryWorker(select.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(TableFilter filter, ObjectNode node) {
        return null;
    }

    @Override
    public QueryWorker createQueryWorker(Call call, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Insert insert, ObjectNode node) {
        return null;
    }

    @Override
    public BatchUpdateWorker createBatchUpdateWorker(Insert insert, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Update update, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Delete delete, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Replace replace, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Merge merge, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Call call, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateTable createTable, ObjectNode node, ObjectNode refNode) {
        SQLTranslated translated = repo.getSQLTranslator().translate(createTable, node, refNode);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(createTable.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropTable dropTable, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(TruncateTable truncateTable, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node, ObjectNode refNode) {
        SQLTranslated translated = repo.getSQLTranslator().translate(alterTableAddConstraint, node, refNode);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(alterTableAddConstraint.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableRename alterTableRename, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableDropConstraint alterTableRename, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateIndex createIndex, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropIndex dropIndex, ObjectNode node) {
        return null;
    }

}
